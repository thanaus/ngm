package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/spf13/cobra"
)

var osSep = string([]byte{os.PathSeparator})

type EntryType int32

const (
	TypeUnknown EntryType = iota
	TypeFile
	TypeDir
	TypeSymlink
	TypeCharDev
	TypeDevice
	TypePipe
	TypeSocket
)

const avroMaxRecordsPerFile = 1_000_000

type ScanResult struct {
	TotalFiles  int64
	TotalDirs   int64
	TotalBytes  int64
	TotalOthers int64
	TotalErrors int64

	IdleNs    int64
	ScanNs    int64
	ReadDirNs int64
	InfoNs    int64
}

type ObjectRecord struct {
	Path        string
	Type        EntryType
	Size        int64
	MTimeUnixNs int64
	CTimeUnixNs int64
	Mode        int64
}

type avroExport struct {
	path string
	rows chan ObjectRecord
	errc chan error
}

type dirQueue struct {
	mu   sync.Mutex
	cond *sync.Cond
	dirs []string
	busy int
}

type stderrPrinter struct {
	mu              sync.Mutex
	progressVisible bool
}

func (p *stderrPrinter) PrintProgress(line string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.progressVisible {
		fmt.Fprint(os.Stderr, "\r")
	}
	fmt.Fprintf(os.Stderr, "%s\033[K", line)
	p.progressVisible = true
}

func (p *stderrPrinter) Println() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.progressVisible {
		return
	}
	fmt.Fprintln(os.Stderr)
	p.progressVisible = false
}

func (p *stderrPrinter) Logf(format string, args ...any) {
	if p == nil {
		log.Printf(format, args...)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	hadProgress := p.progressVisible
	if hadProgress {
		fmt.Fprintln(os.Stderr)
		p.progressVisible = false
	}
	log.Printf(format, args...)
}

func newDirQueue(root string) *dirQueue {
	q := &dirQueue{dirs: []string{root}}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *dirQueue) push(dirs []string) {
	if len(dirs) == 0 {
		return
	}
	q.mu.Lock()
	q.dirs = append(q.dirs, dirs...)
	q.cond.Broadcast()
	q.mu.Unlock()
}

func (q *dirQueue) get() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.dirs) == 0 {
		if q.busy == 0 {
			q.cond.Broadcast()
			return "", false
		}
		q.cond.Wait()
	}
	q.busy++
	last := len(q.dirs) - 1
	dir := q.dirs[last]
	q.dirs[last] = ""
	q.dirs = q.dirs[:last]
	return dir, true
}

func (q *dirQueue) done() {
	q.mu.Lock()
	q.busy--
	if q.busy == 0 && len(q.dirs) == 0 {
		q.cond.Broadcast()
	}
	q.mu.Unlock()
}

func main() {
	var (
		workers    int
		noSize     bool
		batch      int
		cpuProfile string
		memProfile string
		avroDir    string
	)

	rootCmd := &cobra.Command{
		Use:     "nexus-ls <directory>",
		Short:   "Scans a directory tree and reports file statistics",
		Version: "0.8.5",
		Long: `nexus-ls recursively walks a directory using parallel workers
and reports the number of files, directories, total size, and performance metrics.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if workers < 1 {
				return fmt.Errorf("--jobs must be >= 1")
			}
			if batch < 1 {
				return fmt.Errorf("--batch must be >= 1")
			}
			root := filepath.Clean(args[0])
			return run(root, workers, noSize, batch, cpuProfile, memProfile, avroDir)
		},
	}

	rootCmd.Flags().IntVarP(&workers, "jobs", "j", 4, "Number of parallel workers")
	rootCmd.Flags().BoolVarP(&noSize, "no-size", "s", false, "Skip file size collection (avoids one lstat per file)")
	rootCmd.Flags().IntVarP(&batch, "batch", "b", 256, "Number of entries read per ReadDir syscall (getdents64 batch size)")
	rootCmd.Flags().StringVarP(&cpuProfile, "cpuprofile", "p", "", "Write CPU profile to this file (analyse with: go tool pprof -top <file>)")
	rootCmd.Flags().StringVar(&memProfile, "memprofile", "", "Write heap profile to this file (analyse with: go tool pprof -top <binary> <file>)")
	rootCmd.Flags().StringVar(&avroDir, "avro-dir", "", "Write scanned objects to an avro file in this directory")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(root string, workers int, noSize bool, batch int, cpuProfile, memProfile, avroDir string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	printer := &stderrPrinter{}

	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			return fmt.Errorf("could not create CPU profile: %w", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			return fmt.Errorf("could not start CPU profile: %w", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			if err := f.Close(); err != nil {
				printer.Logf("[error] close CPU profile %q: %v", cpuProfile, err)
			}
		}()
	}

	start := time.Now()

	q := newDirQueue(root)
	var result ScanResult
	var wg, reporterWg sync.WaitGroup
	var exportRows chan ObjectRecord
	var exportPath string
	var exportLabel string

	avroExporter, err := startAvroExport(avroDir, workers, batch)
	if err != nil {
		return err
	}
	if avroExporter != nil {
		exportRows = avroExporter.rows
		exportPath = avroExporter.path
		exportLabel = "Avro"
	}

	reporterWg.Add(1)
	go func() {
		defer reporterWg.Done()
		reportProgress(ctx, printer, &result, start, noSize)
	}()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWorker(ctx, q, printer, root, &result, noSize, batch, exportRows)
		}()
	}

	wg.Wait()

	interrupted := ctx.Err() != nil
	if avroExporter != nil {
		close(avroExporter.rows)
	}

	cancel()
	reporterWg.Wait()

	if avroExporter != nil {
		if err := avroExporter.wait(); err != nil {
			return err
		}
	}
	if err := writeDoneFile(avroDir); err != nil {
		return err
	}

	elapsed := time.Since(start)
	dirs := atomic.LoadInt64(&result.TotalDirs)
	files := atomic.LoadInt64(&result.TotalFiles)
	rate := float64(dirs+files) / elapsed.Seconds()

	status := "Scan complete"
	if interrupted {
		status = "Scan interrupted"
	}
	fmt.Printf("\n%s\n\n", status)
	fmt.Printf("%-12s%s\n", "Path", root)
	fmt.Printf("%-12s%s • %s obj/s\n\n", "Duration", compactDuration(elapsed), humanSI(int64(rate)))
	fmt.Printf("%-12s%s dirs • %s files • %s others\n", "Entries",
		humanSI(dirs),
		humanSI(files),
		humanSI(atomic.LoadInt64(&result.TotalOthers)),
	)
	fmt.Printf("%-12s%s\n", "Total size", humanBytes(atomic.LoadInt64(&result.TotalBytes)))
	fmt.Printf("%-12s%d\n\n", "Errors", atomic.LoadInt64(&result.TotalErrors))
	if exportPath != "" {
		fmt.Printf("Output\n")
		fmt.Printf("  %-10s%s\n\n", exportLabel, exportPath)
	}

	totalWorkerNs := float64(workers) * float64(elapsed.Nanoseconds())
	idleNs := float64(atomic.LoadInt64(&result.IdleNs))
	scanNs := float64(atomic.LoadInt64(&result.ScanNs))
	readDirNs := float64(atomic.LoadInt64(&result.ReadDirNs))
	infoNs := float64(atomic.LoadInt64(&result.InfoNs))
	pctIdle := 100 * idleNs / totalWorkerNs
	pctScan := 100 * scanNs / totalWorkerNs

	fmt.Printf("Performance\n")
	fmt.Printf("  %-10s%.1f%% busy • %.1f%% idle\n", "workers", pctScan, pctIdle)
	if scanNs > 0 {
		fmt.Printf("  %-10s%.1f%%\n", "readdir", 100*readDirNs/scanNs)
		fmt.Printf("  %-10s%.1f%%\n", "lstat", 100*infoNs/scanNs)
	}
	if err := writeHeapProfile(memProfile, printer); err != nil {
		return err
	}
	return nil
}

func writeHeapProfile(path string, printer *stderrPrinter) error {
	if path == "" {
		return nil
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("could not create memory profile: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			printer.Logf("[error] close memory profile %q: %v", path, closeErr)
		}
	}()

	// Force a GC so the heap profile reflects live memory.
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		return fmt.Errorf("could not write memory profile: %w", err)
	}
	return nil
}

func reportProgress(ctx context.Context, printer *stderrPrinter, result *ScanResult, start time.Time, noSize bool) {
	const interval = 30 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var prevObjects int64
	for {
		select {
		case <-ctx.Done():
			printer.Println()
			return
		case <-ticker.C:
			dirs := atomic.LoadInt64(&result.TotalDirs)
			files := atomic.LoadInt64(&result.TotalFiles)
			total := dirs + files
			rate := float64(total-prevObjects) / interval.Seconds()
			prevObjects = total

			line := fmt.Sprintf("[%s] %s dirs • %s files • %s obj/s",
				compactDuration(time.Since(start)),
				humanSI(dirs),
				humanSI(files),
				humanSI(int64(rate)),
			)
			if !noSize {
				if b := atomic.LoadInt64(&result.TotalBytes); b > 0 {
					line += " • " + humanBytes(b)
				}
			}

			printer.PrintProgress(line)
		}
	}
}

func runWorker(ctx context.Context, q *dirQueue, printer *stderrPrinter, root string, result *ScanResult, noSize bool, batch int, exportRows chan<- ObjectRecord) {
	for {
		if ctx.Err() != nil {
			return
		}

		t0 := time.Now()
		dir, ok := q.get()
		atomic.AddInt64(&result.IdleNs, int64(time.Since(t0)))

		if !ok {
			return
		}

		t1 := time.Now()
		subdirs := scanDir(ctx, printer, root, dir, result, noSize, batch, exportRows)
		atomic.AddInt64(&result.ScanNs, int64(time.Since(t1)))

		q.push(subdirs)
		q.done()
	}
}

func scanDir(ctx context.Context, printer *stderrPrinter, root, dir string, result *ScanResult, noSize bool, batch int, exportRows chan<- ObjectRecord) []string {
	f, err := os.Open(dir)
	if err != nil {
		printer.Logf("[error] open %q: %v", dir, err)
		atomic.AddInt64(&result.TotalErrors, 1)
		return nil
	}
	defer f.Close()

	var subdirs []string

	prefix := dir
	if dir[len(dir)-1] != os.PathSeparator {
		prefix = dir + osSep
	}

	for ctx.Err() == nil {
		t := time.Now()
		entries, err := f.ReadDir(batch)
		atomic.AddInt64(&result.ReadDirNs, int64(time.Since(t)))

		for _, entry := range entries {
			path := prefix + entry.Name()
			relativePath := exportRelativePath(root, path)
			t := time.Now()
			info, infoErr := entry.Info()
			atomic.AddInt64(&result.InfoNs, int64(time.Since(t)))
			if infoErr != nil {
				printer.Logf("[error] info %q: %v", path, infoErr)
				atomic.AddInt64(&result.TotalErrors, 1)
			}
			switch {
			case entry.IsDir():
				atomic.AddInt64(&result.TotalDirs, 1)
				subdirs = append(subdirs, path)
				record := ObjectRecord{Path: relativePath, Type: TypeDir}
				if infoErr == nil {
					fillRecordFromInfo(&record, info)
				}
				if !emitRecord(ctx, exportRows, record) {
					return subdirs
				}

			case entry.Type().IsRegular():
				atomic.AddInt64(&result.TotalFiles, 1)
				record := ObjectRecord{Path: relativePath, Type: TypeFile}
				if infoErr == nil {
					fillRecordFromInfo(&record, info)
					atomic.AddInt64(&result.TotalBytes, record.Size)
				}
				if !emitRecord(ctx, exportRows, record) {
					return subdirs
				}

			default:
				atomic.AddInt64(&result.TotalOthers, 1)
				record := ObjectRecord{Path: relativePath, Type: entryTypeFromMode(entry.Type())}
				if infoErr == nil {
					fillRecordFromInfo(&record, info)
				}
				if !emitRecord(ctx, exportRows, record) {
					return subdirs
				}
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				printer.Logf("[error] readdir %q: %v", dir, err)
				atomic.AddInt64(&result.TotalErrors, 1)
			}
			break
		}
	}

	return subdirs
}

func emitRecord(ctx context.Context, exportRows chan<- ObjectRecord, record ObjectRecord) bool {
	if exportRows == nil {
		return true
	}
	select {
	case exportRows <- record:
		return true
	case <-ctx.Done():
		return false
	}
}

func startAvroExport(avroDir string, workers, batch int) (*avroExport, error) {
	if avroDir == "" {
		return nil, nil
	}
	tmpDir := filepath.Join(avroDir, "tmp")
	incomingDir := filepath.Join(avroDir, "incoming")
	processingDir := filepath.Join(avroDir, "processing")
	doneDir := filepath.Join(avroDir, "done")
	errorDir := filepath.Join(avroDir, "error")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, fmt.Errorf("could not create avro tmp directory %q: %w", tmpDir, err)
	}
	if err := os.MkdirAll(incomingDir, 0o755); err != nil {
		return nil, fmt.Errorf("could not create avro incoming directory %q: %w", incomingDir, err)
	}
	if err := os.MkdirAll(processingDir, 0o755); err != nil {
		return nil, fmt.Errorf("could not create avro processing directory %q: %w", processingDir, err)
	}
	if err := os.MkdirAll(doneDir, 0o755); err != nil {
		return nil, fmt.Errorf("could not create avro done directory %q: %w", doneDir, err)
	}
	if err := os.MkdirAll(errorDir, 0o755); err != nil {
		return nil, fmt.Errorf("could not create avro error directory %q: %w", errorDir, err)
	}

	baseName := fmt.Sprintf("scan-%s", time.Now().Format("20060102-150405"))
	pathPattern := filepath.Join(incomingDir, baseName+"-*.avro")

	exporter := &avroExport{
		path: pathPattern,
		rows: make(chan ObjectRecord, exportBufferSize(workers, batch)),
		errc: make(chan error, 1),
	}
	go func() {
		exporter.errc <- writeAvroFiles(tmpDir, incomingDir, baseName, exporter.rows)
	}()
	return exporter, nil
}

func (e *avroExport) wait() error {
	if e == nil {
		return nil
	}
	return <-e.errc
}

func writeDoneFile(avroDir string) error {
	if avroDir == "" {
		return nil
	}

	donePath := filepath.Join(avroDir, "incoming", ".done")
	f, err := os.Create(donePath)
	if err != nil {
		return fmt.Errorf("could not create done file %q: %w", donePath, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("could not close done file %q: %w", donePath, err)
	}
	return nil
}

func exportBufferSize(workers, batch int) int {
	bufferSize := workers * batch
	if bufferSize < 1024 {
		bufferSize = 1024
	}
	return bufferSize
}

func exportRelativePath(root, path string) string {
	relativePath, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(relativePath)
}

func fillRecordFromInfo(record *ObjectRecord, info os.FileInfo) {
	record.Type = entryTypeFromMode(info.Mode())
	record.Size = info.Size()
	record.MTimeUnixNs = info.ModTime().UnixNano()
	record.CTimeUnixNs = ctimeUnixNs(info)
	record.Mode = int64(info.Mode())
}

func entryTypeFromMode(mode os.FileMode) EntryType {
	switch {
	case mode.IsRegular():
		return TypeFile
	case mode.IsDir():
		return TypeDir
	case mode&os.ModeSymlink != 0:
		return TypeSymlink
	case mode&os.ModeSocket != 0:
		return TypeSocket
	case mode&os.ModeNamedPipe != 0:
		return TypePipe
	case mode&os.ModeDevice != 0 && mode&os.ModeCharDevice != 0:
		return TypeCharDev
	case mode&os.ModeDevice != 0:
		return TypeDevice
	default:
		return TypeUnknown
	}
}

func ctimeUnixNs(info os.FileInfo) int64 {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return stat.Ctim.Sec*1_000_000_000 + stat.Ctim.Nsec
}

func writeAvroFiles(tmpDir, incomingDir, baseName string, rows <-chan ObjectRecord) (err error) {
	var (
		f                   *os.File
		writer              *goavro.OCFWriter
		fileIndex           int
		recordsInFile       int
		currentTmpPath      string
		currentIncomingPath string
	)

	closeCurrent := func() error {
		if f == nil {
			return nil
		}
		closeErr := f.Close()
		name := f.Name()
		f = nil
		writer = nil
		if closeErr != nil {
			return fmt.Errorf("could not close avro file %q: %w", name, closeErr)
		}
		return nil
	}

	publishCurrent := func() error {
		if f == nil {
			return nil
		}
		if err := closeCurrent(); err != nil {
			return err
		}
		if err := os.Rename(currentTmpPath, currentIncomingPath); err != nil {
			return fmt.Errorf("could not move avro file %q to %q: %w", currentTmpPath, currentIncomingPath, err)
		}
		currentTmpPath = ""
		currentIncomingPath = ""
		recordsInFile = 0
		return nil
	}

	defer func() {
		if closeErr := closeCurrent(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = errors.Join(err, closeErr)
			}
		}
	}()

	openNext := func() error {
		if err := closeCurrent(); err != nil {
			return err
		}
		fileIndex++
		recordsInFile = 0
		currentTmpPath = filepath.Join(tmpDir, fmt.Sprintf("%s-%06d.avro", baseName, fileIndex))
		currentIncomingPath = filepath.Join(incomingDir, fmt.Sprintf("%s-%06d.avro", baseName, fileIndex))
		nextFile, err := os.Create(currentTmpPath)
		if err != nil {
			return fmt.Errorf("could not create avro file %q: %w", currentTmpPath, err)
		}
		nextWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
			W:               nextFile,
			Schema:          avroObjectRecordSchema,
			CompressionName: "deflate",
		})
		if err != nil {
			closeErr := nextFile.Close()
			if closeErr != nil {
				return errors.Join(
					fmt.Errorf("could not initialize avro writer %q: %w", currentTmpPath, err),
					fmt.Errorf("could not close avro file %q: %w", currentTmpPath, closeErr),
				)
			}
			return fmt.Errorf("could not initialize avro writer %q: %w", currentTmpPath, err)
		}
		f = nextFile
		writer = nextWriter
		return nil
	}

	batch := make([]interface{}, 0, 1024)
	flush := func() error {
		pending := batch
		for len(pending) > 0 {
			if writer == nil {
				if err := openNext(); err != nil {
					return err
				}
			}

			remaining := avroMaxRecordsPerFile - recordsInFile
			if remaining <= 0 {
				if err := publishCurrent(); err != nil {
					return err
				}
				if err := openNext(); err != nil {
					return err
				}
				remaining = avroMaxRecordsPerFile
			}
			if remaining > len(pending) {
				remaining = len(pending)
			}

			if err := writer.Append(pending[:remaining]); err != nil {
				return fmt.Errorf("could not write avro rows to %q: %w", f.Name(), err)
			}
			recordsInFile += remaining
			pending = pending[remaining:]
		}
		batch = batch[:0]
		return nil
	}

	for row := range rows {
		batch = append(batch, map[string]interface{}{
			"path":          row.Path,
			"entry_type":    int32(row.Type),
			"size":          row.Size,
			"mtime_unix_ns": row.MTimeUnixNs,
			"ctime_unix_ns": row.CTimeUnixNs,
			"mode":          row.Mode,
		})
		if len(batch) == cap(batch) {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	if err := flush(); err != nil {
		return err
	}
	return publishCurrent()
}

const avroObjectRecordSchema = `{
  "type": "record",
  "name": "ObjectRecord",
  "fields": [
    {"name": "path", "type": "string"},
    {"name": "entry_type", "type": "int"},
    {"name": "size", "type": "long"},
    {"name": "mtime_unix_ns", "type": "long"},
    {"name": "ctime_unix_ns", "type": "long"},
    {"name": "mode", "type": "long"}
  ]
}`

func humanSI(n int64) string {
	switch {
	case n < 1_000:
		return fmt.Sprintf("%d", n)
	case n < 1_000_000:
		return fmt.Sprintf("%.1fk", float64(n)/1_000)
	default:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	}
}

func compactDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	switch {
	case h > 0:
		return fmt.Sprintf("%dh%dm%ds", h, m, s)
	case m > 0:
		return fmt.Sprintf("%dm%ds", m, s)
	default:
		return fmt.Sprintf("%ds", s)
	}
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
