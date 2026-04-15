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
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/spf13/cobra"
)

var osSep = string([]byte{os.PathSeparator})

const (
	objectKindDir   = "dir"
	objectKindFile  = "file"
	objectKindOther = "other"
)

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
	Path string
	Kind string
	Size int64
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

	rootCmd.Flags().IntVarP(&workers,       "jobs",       "j", 4,     "Number of parallel workers")
	rootCmd.Flags().BoolVarP(&noSize,       "no-size",    "s", false, "Skip file size collection (avoids one lstat per file)")
	rootCmd.Flags().IntVarP(&batch,         "batch",      "b", 256,   "Number of entries read per ReadDir syscall (getdents64 batch size)")
	rootCmd.Flags().StringVarP(&cpuProfile, "cpuprofile", "p", "",    "Write CPU profile to this file (analyse with: go tool pprof -top <file>)")
	rootCmd.Flags().StringVar(&memProfile,  "memprofile", "",         "Write heap profile to this file (analyse with: go tool pprof -top <binary> <file>)")
	rootCmd.Flags().StringVar(&avroDir,     "avro-dir", "",           "Write scanned objects to an avro file in this directory")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(root string, workers int, noSize bool, batch int, cpuProfile, memProfile, avroDir string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

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
				log.Printf("[error] close CPU profile %q: %v", cpuProfile, err)
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
		reportProgress(ctx, &result, start, noSize)
	}()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWorker(ctx, q, &result, noSize, batch, exportRows)
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

	elapsed := time.Since(start)
	dirs  := atomic.LoadInt64(&result.TotalDirs)
	files := atomic.LoadInt64(&result.TotalFiles)
	rate  := float64(dirs+files) / elapsed.Seconds()

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
	fmt.Printf("%-12s%d\n", "Errors", atomic.LoadInt64(&result.TotalErrors))
	fmt.Printf("%-12s%s\n\n", "Size", humanBytes(atomic.LoadInt64(&result.TotalBytes)))
	if exportPath != "" {
		fmt.Printf("%-12s%s\n\n", exportLabel, exportPath)
	}

	totalWorkerNs := float64(workers) * float64(elapsed.Nanoseconds())
	idleNs        := float64(atomic.LoadInt64(&result.IdleNs))
	scanNs        := float64(atomic.LoadInt64(&result.ScanNs))
	readDirNs     := float64(atomic.LoadInt64(&result.ReadDirNs))
	infoNs        := float64(atomic.LoadInt64(&result.InfoNs))
	pctIdle       := 100 * idleNs / totalWorkerNs
	pctScan       := 100 * scanNs / totalWorkerNs

	fmt.Printf("Performance\n")
	fmt.Printf("  %-10s%.1f%% busy • %.1f%% idle\n", "workers", pctScan, pctIdle)
	if scanNs > 0 {
		fmt.Printf("  %-10s%.1f%%\n", "readdir", 100*readDirNs/scanNs)
		fmt.Printf("  %-10s%.1f%%\n", "lstat", 100*infoNs/scanNs)
	}
	if err := writeHeapProfile(memProfile); err != nil {
		return err
	}
	return nil
}

func writeHeapProfile(path string) error {
	if path == "" {
		return nil
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("could not create memory profile: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("[error] close memory profile %q: %v", path, closeErr)
		}
	}()

	// Force a GC so the heap profile reflects live memory.
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		return fmt.Errorf("could not write memory profile: %w", err)
	}
	return nil
}

func reportProgress(ctx context.Context, result *ScanResult, start time.Time, noSize bool) {
	const interval = 30 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var prevObjects int64
	firstPrint := true
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintf(os.Stderr, "\n")
			return
		case <-ticker.C:
			dirs  := atomic.LoadInt64(&result.TotalDirs)
			files := atomic.LoadInt64(&result.TotalFiles)
			total := dirs + files
			rate  := float64(total-prevObjects) / interval.Seconds()
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

			if !firstPrint {
				fmt.Fprintf(os.Stderr, "\r")
			}
			fmt.Fprintf(os.Stderr, "%s\033[K", line)
			firstPrint = false
		}
	}
}

func runWorker(ctx context.Context, q *dirQueue, result *ScanResult, noSize bool, batch int, exportRows chan<- ObjectRecord) {
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
		subdirs := scanDir(ctx, dir, result, noSize, batch, exportRows)
		atomic.AddInt64(&result.ScanNs, int64(time.Since(t1)))

		q.push(subdirs)
		q.done()
	}
}

func scanDir(ctx context.Context, dir string, result *ScanResult, noSize bool, batch int, exportRows chan<- ObjectRecord) []string {
	f, err := os.Open(dir)
	if err != nil {
		log.Printf("[error] open %q: %v", dir, err)
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
			switch {
			case entry.IsDir():
				atomic.AddInt64(&result.TotalDirs, 1)
				subdirs = append(subdirs, path)
				if !emitRecord(ctx, exportRows, ObjectRecord{Path: path, Kind: objectKindDir}) {
					return subdirs
				}

			case entry.Type().IsRegular():
				atomic.AddInt64(&result.TotalFiles, 1)
				record := ObjectRecord{Path: path, Kind: objectKindFile}
				if !noSize {
					t := time.Now()
					info, infoErr := entry.Info()
					atomic.AddInt64(&result.InfoNs, int64(time.Since(t)))
					if infoErr != nil {
						log.Printf("[error] info %q: %v", path, infoErr)
						atomic.AddInt64(&result.TotalErrors, 1)
					} else {
						record.Size = info.Size()
						atomic.AddInt64(&result.TotalBytes, record.Size)
					}
				}
				if !emitRecord(ctx, exportRows, record) {
					return subdirs
				}

			default:
				atomic.AddInt64(&result.TotalOthers, 1)
				if !emitRecord(ctx, exportRows, ObjectRecord{Path: path, Kind: objectKindOther}) {
					return subdirs
				}
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("[error] readdir %q: %v", dir, err)
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
	if err := os.MkdirAll(avroDir, 0o755); err != nil {
		return nil, fmt.Errorf("could not create avro directory %q: %w", avroDir, err)
	}

	path := filepath.Join(avroDir, fmt.Sprintf("scan-%s.avro", time.Now().Format("20060102-150405")))
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("could not create avro file %q: %w", path, err)
	}

	exporter := &avroExport{
		path: path,
		rows: make(chan ObjectRecord, exportBufferSize(workers, batch)),
		errc: make(chan error, 1),
	}
	go func() {
		exporter.errc <- writeAvroFile(f, exporter.rows)
	}()
	return exporter, nil
}

func (e *avroExport) wait() error {
	if e == nil {
		return nil
	}
	return <-e.errc
}

func exportBufferSize(workers, batch int) int {
	bufferSize := workers * batch
	if bufferSize < 1024 {
		bufferSize = 1024
	}
	return bufferSize
}

func writeAvroFile(f *os.File, rows <-chan ObjectRecord) (err error) {
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			closeErr = fmt.Errorf("could not close avro file %q: %w", f.Name(), closeErr)
			if err == nil {
				err = closeErr
			} else {
				err = errors.Join(err, closeErr)
			}
		}
	}()

	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               f,
		Schema:          avroObjectRecordSchema,
		CompressionName: "deflate",
	})
	if err != nil {
		return fmt.Errorf("could not initialize avro writer %q: %w", f.Name(), err)
	}

	batch := make([]interface{}, 0, 1024)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := writer.Append(batch); err != nil {
			return fmt.Errorf("could not write avro rows to %q: %w", f.Name(), err)
		}
		batch = batch[:0]
		return nil
	}

	for row := range rows {
		batch = append(batch, map[string]interface{}{
			"path": row.Path,
			"kind": row.Kind,
			"size": row.Size,
		})
		if len(batch) == cap(batch) {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	return flush()
}

const avroObjectRecordSchema = `{
  "type": "record",
  "name": "ObjectRecord",
  "fields": [
    {"name": "path", "type": "string"},
    {"name": "kind", "type": "string"},
    {"name": "size", "type": "long"}
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
