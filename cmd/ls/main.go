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
	"regexp"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"nexus-ls/internal/avro"

	"github.com/spf13/cobra"
)

var osSep = string([]byte{os.PathSeparator})

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

type pathMatcher struct {
	includes []*regexp.Regexp
	excludes []*regexp.Regexp
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
		workers         int
		noSize          bool
		batch           int
		cpuProfile      string
		memProfile      string
		avroDir         string
		excludePatterns []string
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
			matcher, err := newPathMatcher(nil, excludePatterns)
			if err != nil {
				return err
			}
			root := filepath.Clean(args[0])
			return run(root, workers, noSize, batch, cpuProfile, memProfile, avroDir, matcher)
		},
	}

	rootCmd.Flags().IntVarP(&workers, "jobs", "j", 4, "Number of parallel workers")
	rootCmd.Flags().BoolVarP(&noSize, "no-size", "s", false, "Skip file size collection (avoids one lstat per file)")
	rootCmd.Flags().IntVarP(&batch, "batch", "b", 256, "Number of entries read per ReadDir syscall (getdents64 batch size)")
	rootCmd.Flags().StringArrayVar(&excludePatterns, "exclude", nil, "Exclude relative paths matching this regular expression (repeatable; Go regexp syntax)")
	rootCmd.Flags().StringVarP(&cpuProfile, "cpuprofile", "p", "", "Write CPU profile to this file (analyse with: go tool pprof -top <file>)")
	rootCmd.Flags().StringVar(&memProfile, "memprofile", "", "Write heap profile to this file (analyse with: go tool pprof -top <binary> <file>)")
	rootCmd.Flags().StringVar(&avroDir, "avro-dir", "", "Write scanned objects to an avro file in this directory")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(root string, workers int, noSize bool, batch int, cpuProfile, memProfile, avroDir string, matcher *pathMatcher) error {
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
	var exportRows chan avro.Record
	var exportPath string
	var exportLabel string

	avroExporter, err := avro.StartExport(avroDir, workers, batch)
	if err != nil {
		return err
	}
	if avroExporter != nil {
		exportRows = avroExporter.Rows
		exportPath = avroExporter.PathPattern()
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
			runWorker(ctx, q, printer, root, &result, noSize, batch, exportRows, matcher)
		}()
	}

	wg.Wait()

	interrupted := ctx.Err() != nil
	if avroExporter != nil {
		close(avroExporter.Rows)
	}

	cancel()
	reporterWg.Wait()

	if avroExporter != nil {
		if err := avroExporter.Wait(); err != nil {
			return err
		}
	}
	if err := avro.WriteDoneFile(avroDir); err != nil {
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

func runWorker(ctx context.Context, q *dirQueue, printer *stderrPrinter, root string, result *ScanResult, noSize bool, batch int, exportRows chan<- avro.Record, matcher *pathMatcher) {
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
		subdirs := scanDir(ctx, printer, root, dir, result, noSize, batch, exportRows, matcher)
		atomic.AddInt64(&result.ScanNs, int64(time.Since(t1)))

		q.push(subdirs)
		q.done()
	}
}

func scanDir(ctx context.Context, printer *stderrPrinter, root, dir string, result *ScanResult, noSize bool, batch int, exportRows chan<- avro.Record, matcher *pathMatcher) []string {
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
			if matcher != nil && !matcher.allow(relativePath, entry.IsDir()) {
				continue
			}
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
				record := avro.Record{Path: relativePath, Type: avro.TypeDir}
				if infoErr == nil {
					fillRecordFromInfo(&record, info)
				}
				if !emitRecord(ctx, exportRows, record) {
					return subdirs
				}

			case entry.Type().IsRegular():
				atomic.AddInt64(&result.TotalFiles, 1)
				record := avro.Record{Path: relativePath, Type: avro.TypeFile}
				if infoErr == nil {
					fillRecordFromInfo(&record, info)
					atomic.AddInt64(&result.TotalBytes, record.Size)
				}
				if !emitRecord(ctx, exportRows, record) {
					return subdirs
				}

			default:
				atomic.AddInt64(&result.TotalOthers, 1)
				record := avro.Record{Path: relativePath, Type: entryTypeFromMode(entry.Type())}
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

func newPathMatcher(includePatterns, excludePatterns []string) (*pathMatcher, error) {
	includes, err := compilePatterns("include", includePatterns)
	if err != nil {
		return nil, err
	}
	excludes, err := compilePatterns("exclude", excludePatterns)
	if err != nil {
		return nil, err
	}
	return &pathMatcher{
		includes: includes,
		excludes: excludes,
	}, nil
}

func compilePatterns(kind string, patterns []string) ([]*regexp.Regexp, error) {
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for _, pattern := range patterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid %s regexp %q: %w", kind, pattern, err)
		}
		compiled = append(compiled, re)
	}
	return compiled, nil
}

func (m *pathMatcher) allow(path string, isDir bool) bool {
	if m == nil {
		return true
	}
	if len(m.includes) > 0 && !matchesAnyPattern(m.includes, path, isDir) {
		return false
	}
	return !matchesAnyPattern(m.excludes, path, isDir)
}

func matchesAnyPattern(patterns []*regexp.Regexp, path string, isDir bool) bool {
	for _, pattern := range patterns {
		if pattern.MatchString(path) {
			return true
		}
		if isDir && pattern.MatchString(path+"/") {
			return true
		}
	}
	return false
}

func emitRecord(ctx context.Context, exportRows chan<- avro.Record, record avro.Record) bool {
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

func exportRelativePath(root, path string) string {
	relativePath, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(relativePath)
}

func fillRecordFromInfo(record *avro.Record, info os.FileInfo) {
	record.Type = entryTypeFromMode(info.Mode())
	record.Size = info.Size()
	record.MTimeUnixNs = info.ModTime().UnixNano()
	record.CTimeUnixNs = ctimeUnixNs(info)
	record.Mode = int64(info.Mode())
}

func entryTypeFromMode(mode os.FileMode) avro.EntryType {
	switch {
	case mode.IsRegular():
		return avro.TypeFile
	case mode.IsDir():
		return avro.TypeDir
	case mode&os.ModeSymlink != 0:
		return avro.TypeSymlink
	case mode&os.ModeSocket != 0:
		return avro.TypeSocket
	case mode&os.ModeNamedPipe != 0:
		return avro.TypePipe
	case mode&os.ModeDevice != 0 && mode&os.ModeCharDevice != 0:
		return avro.TypeCharDev
	case mode&os.ModeDevice != 0:
		return avro.TypeDevice
	default:
		return avro.TypeUnknown
	}
}

func ctimeUnixNs(info os.FileInfo) int64 {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return stat.Ctim.Sec*1_000_000_000 + stat.Ctim.Nsec
}

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
