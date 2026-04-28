package main

import (
	"bufio"
	"bytes"
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
	"runtime/trace"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"nexus-ls/internal/avro"

	"github.com/spf13/cobra"
)

type CopyContext struct {
	SourceDir      string
	DestinationDir string
	AvroDir        string
	Workers        int
	Verbosity      int
	LogFile        string
	DecisionLog    *decisionLogger
}

type DirectoryMetadata struct {
	Mode         os.FileMode
	UID          int
	GID          int
	Atime        time.Time
	MTime        time.Time
	Xattrs       map[string][]byte
	HasOwnership bool
}

type AvroStats struct {
	TotalDirs   int64
	TotalFiles  int64
	TotalOthers int64
	TotalBytes  int64
}

type ProfileConfig struct {
	CPUProfile       string
	MemProfile       string
	BlockProfile     string
	MutexProfile     string
	GoroutineProfile string
	TraceFile        string
}

const ensuredParentCacheSize = 128
const incomingPollInterval = 10 * time.Second

type workerState struct {
	ensuredParents *recentPathSet
}

type recentPathSet struct {
	capacity int
	paths    []string
	index    map[string]int
}

type profiler struct {
	cfg       ProfileConfig
	printer   *stderrPrinter
	cpuFile   *os.File
	traceFile *os.File
}

type decisionLogger struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
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

	if p.progressVisible {
		fmt.Fprintln(os.Stderr)
		p.progressVisible = false
	}
	log.Printf(format, args...)
}

func newWorkerState() *workerState {
	return &workerState{
		ensuredParents: newRecentPathSet(ensuredParentCacheSize),
	}
}

func newRecentPathSet(capacity int) *recentPathSet {
	return &recentPathSet{
		capacity: capacity,
		index:    make(map[string]int, capacity),
	}
}

func (s *recentPathSet) has(path string) bool {
	_, ok := s.index[path]
	return ok
}

func (s *recentPathSet) add(path string) {
	if s.capacity <= 0 || path == "" || s.has(path) {
		return
	}
	if len(s.paths) == s.capacity {
		evicted := s.paths[0]
		delete(s.index, evicted)
		copy(s.paths, s.paths[1:])
		s.paths[len(s.paths)-1] = path
		for i, existing := range s.paths {
			s.index[existing] = i
		}
		return
	}
	s.paths = append(s.paths, path)
	s.index[path] = len(s.paths) - 1
}

type workerErrorSink struct {
	mu    sync.Mutex
	once  sync.Once
	err   error
	count int64
	logf  func(error)
}

func (s *workerErrorSink) set(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	s.count++
	logf := s.logf
	s.mu.Unlock()

	s.once.Do(func() {
		s.err = err
	})
	if logf != nil {
		logf(err)
	}
}

func (s *workerErrorSink) errorCount() int64 {
	if s == nil {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func (ctx CopyContext) sourcePathFor(recordPath string) string {
	return filepath.Join(ctx.SourceDir, filepath.FromSlash(recordPath))
}

func (ctx CopyContext) destinationPathFor(recordPath string) string {
	return filepath.Join(ctx.DestinationDir, filepath.FromSlash(recordPath))
}

func main() {
	var (
		avroDir          string
		workers          int
		verbosity        int
		logFile          string
		cpuProfile       string
		memProfile       string
		blockProfile     string
		mutexProfile     string
		goroutineProfile string
		traceFile        string
	)

	rootCmd := &cobra.Command{
		Use:     "nexus-copy <source-directory> <destination-directory>",
		Short:   "Copies a directory tree from source to destination",
		Version: "0.1.0",
		Long: `nexus-copy prepares a directory copy operation from a source
directory to a destination directory.

This is only a skeleton for now and the actual copy logic will be added later.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if workers < 1 {
				return fmt.Errorf("--jobs must be >= 1")
			}

			sourceDir := filepath.Clean(args[0])
			destinationDir := filepath.Clean(args[1])

			if err := validateSourceDir(sourceDir); err != nil {
				return err
			}
			if err := validateDestinationDir(destinationDir); err != nil {
				return err
			}
			if err := validateAvroDir(avroDir); err != nil {
				return err
			}
			if verbosity >= 2 && logFile == "" {
				return fmt.Errorf("--log-file is required with -vv")
			}

			return run(CopyContext{
				SourceDir:      sourceDir,
				DestinationDir: destinationDir,
				AvroDir:        avroDir,
				Workers:        workers,
				Verbosity:      verbosity,
				LogFile:        logFile,
			}, ProfileConfig{
				CPUProfile:       cpuProfile,
				MemProfile:       memProfile,
				BlockProfile:     blockProfile,
				MutexProfile:     mutexProfile,
				GoroutineProfile: goroutineProfile,
				TraceFile:        traceFile,
			})
		},
	}

	rootCmd.Flags().IntVarP(&workers, "jobs", "j", 4, "Number of parallel workers")
	rootCmd.Flags().CountVarP(&verbosity, "verbose", "v", "Increase verbosity (-vv enables per-object decision logging)")
	rootCmd.Flags().StringVar(&logFile, "log-file", "", "Write -vv decision logs to this file")
	rootCmd.Flags().StringVarP(&cpuProfile, "cpuprofile", "p", "", "Write CPU profile to this file")
	rootCmd.Flags().StringVar(&memProfile, "memprofile", "", "Write heap profile to this file")
	rootCmd.Flags().StringVar(&blockProfile, "blockprofile", "", "Write blocking profile to this file")
	rootCmd.Flags().StringVar(&mutexProfile, "mutexprofile", "", "Write mutex profile to this file")
	rootCmd.Flags().StringVar(&goroutineProfile, "goroutineprofile", "", "Write goroutine profile to this file")
	rootCmd.Flags().StringVar(&traceFile, "trace", "", "Write execution trace to this file")
	rootCmd.Flags().StringVar(
		&avroDir,
		"avro-dir",
		"",
		"Existing directory containing tmp and incoming subdirectories",
	)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(ctx CopyContext, profileCfg ProfileConfig) (err error) {
	runCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	printer := &stderrPrinter{}
	profiler, err := startProfiler(profileCfg, printer)
	if err != nil {
		return err
	}
	defer func() {
		if stopErr := profiler.Stop(); stopErr != nil && err == nil {
			err = stopErr
		}
	}()

	if ctx.Verbosity >= 2 {
		logger, logErr := newDecisionLogger(ctx.LogFile)
		if logErr != nil {
			return logErr
		}
		ctx.DecisionLog = logger
		defer func() {
			if closeErr := logger.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
		}()
	}

	if ctx.AvroDir != "" {
		return processIncomingAvroFiles(runCtx, ctx)
	}
	fmt.Println()
	fmt.Println("Copy skeleton ready. Actual copy logic is not implemented yet.")
	return nil
}

func validateSourceDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("could not access source directory %q: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("source path %q is not a directory", path)
	}
	return nil
}

func validateDestinationDir(path string) error {
	if path == "" {
		return fmt.Errorf("destination directory is required")
	}
	return nil
}

func validateAvroDir(path string) error {
	return avro.ValidateDir(path)
}

func processIncomingAvroFiles(runCtx context.Context, ctx CopyContext) error {
	totalStart := time.Now()
	var totalStats AvroStats
	var totalErrors int64
	totalPrinted := false

scanLoop:
	for {
		if runCtx.Err() != nil {
			break
		}

		claim, hasDone, err := avro.ClaimNextIncomingFile(ctx.AvroDir)
		if err != nil {
			if runCtx.Err() != nil {
				break
			}
			return err
		}
		if claim != nil {
			shardStats, shardErrorCount, processErr, fatalErr := processOneIncomingAvroFile(runCtx, ctx, claim)
			if fatalErr != nil {
				return fatalErr
			}
			totalStats.TotalDirs += shardStats.TotalDirs
			totalStats.TotalFiles += shardStats.TotalFiles
			totalStats.TotalOthers += shardStats.TotalOthers
			totalStats.TotalBytes += shardStats.TotalBytes
			totalErrors += shardErrorCount
			fmt.Printf("[total %s] %s\n\n", compactDuration(time.Since(totalStart)), formatAvroSummary(&totalStats))
			totalPrinted = true
			if errors.Is(processErr, context.Canceled) {
				break
			}
			continue
		}
		if hasDone {
			if !totalPrinted {
				fmt.Printf("[total %s] %s\n", compactDuration(time.Since(totalStart)), formatAvroSummary(&totalStats))
			}
			printCopySummary(ctx, totalStart, &totalStats, totalErrors)
			return nil
		}

		timer := time.NewTimer(incomingPollInterval)
		select {
		case <-runCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			break scanLoop
		case <-timer.C:
		}
	}

	if !totalPrinted {
		fmt.Printf("[total %s] %s\n", compactDuration(time.Since(totalStart)), formatAvroSummary(&totalStats))
	}
	printCopySummaryWithStatus(ctx, totalStart, &totalStats, totalErrors, true)
	return nil
}

func processOneIncomingAvroFile(runCtx context.Context, ctx CopyContext, claim *avro.ClaimedFile) (stats AvroStats, errorCount int64, processErr error, fatalErr error) {
	fmt.Printf("[start] %s\n", claim.SelectedName)
	start := time.Now()
	printer := &stderrPrinter{}
	progressCtx, cancelProgress := context.WithCancel(runCtx)
	var progressWg sync.WaitGroup
	progressWg.Add(1)
	go func() {
		defer progressWg.Done()
		reportAvroProgress(progressCtx, printer, &stats, start)
	}()

	processErr, errorCount = processAvroPaths(runCtx, ctx, claim.ProcessingPath, &stats, &workerErrorSink{
		logf: func(err error) {
			printer.Logf("[error] processing %q: %v", claim.SelectedName, err)
		},
	})
	cancelProgress()
	progressWg.Wait()

	targetLabel, err := avro.FinalizeClaim(ctx.AvroDir, claim, processErr != nil)
	if err != nil {
		moveErr := err
		errorCount++
		if processErr != nil {
			return stats, errorCount, nil, errors.Join(processErr, moveErr)
		}
		return stats, errorCount, nil, moveErr
	}

	fmt.Printf("[%s %s] %s\n", targetLabel, compactDuration(time.Since(start)), formatAvroSummary(&stats))
	return stats, errorCount, processErr, nil
}

func processAvroPaths(runCtx context.Context, ctx CopyContext, path string, stats *AvroStats, workerErrors *workerErrorSink) (error, int64) {
	records := make(chan avro.Record, recordBufferSize(ctx.Workers))
	var wg sync.WaitGroup

	if workerErrors == nil {
		workerErrors = &workerErrorSink{}
	}

	for i := 0; i < ctx.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			copyWorker(runCtx, ctx, newWorkerState(), records, stats, workerErrors)
		}()
	}

	readErr := avro.ReadRecords(runCtx, path, records)
	close(records)
	wg.Wait()
	errorCount := workerErrors.errorCount()
	if readErr != nil && !errors.Is(readErr, context.Canceled) {
		errorCount++
	}
	if errors.Is(readErr, context.Canceled) {
		if workerErrors.err != nil {
			return errors.Join(runCtx.Err(), workerErrors.err), errorCount
		}
		return runCtx.Err(), errorCount
	}

	if readErr != nil && workerErrors.err != nil {
		return errors.Join(readErr, workerErrors.err), errorCount
	}
	if readErr != nil {
		return readErr, errorCount
	}
	return workerErrors.err, errorCount
}

func copyWorker(runCtx context.Context, ctx CopyContext, state *workerState, records <-chan avro.Record, stats *AvroStats, workerErrors *workerErrorSink) {
	for {
		select {
		case <-runCtx.Done():
			return
		case record, ok := <-records:
			if !ok {
				return
			}
			if err := processRecord(ctx, state, record, stats); err != nil {
				workerErrors.set(err)
			}
		}
	}
}

func startProfiler(cfg ProfileConfig, printer *stderrPrinter) (*profiler, error) {
	p := &profiler{
		cfg:     cfg,
		printer: printer,
	}

	if cfg.BlockProfile != "" {
		runtime.SetBlockProfileRate(1)
	}
	if cfg.MutexProfile != "" {
		runtime.SetMutexProfileFraction(1)
	}

	if cfg.CPUProfile != "" {
		f, err := os.Create(cfg.CPUProfile)
		if err != nil {
			runtime.SetBlockProfileRate(0)
			runtime.SetMutexProfileFraction(0)
			return nil, fmt.Errorf("could not create CPU profile %q: %w", cfg.CPUProfile, err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			runtime.SetBlockProfileRate(0)
			runtime.SetMutexProfileFraction(0)
			return nil, fmt.Errorf("could not start CPU profile %q: %w", cfg.CPUProfile, err)
		}
		p.cpuFile = f
	}

	if cfg.TraceFile != "" {
		f, err := os.Create(cfg.TraceFile)
		if err != nil {
			_ = p.Stop()
			return nil, fmt.Errorf("could not create trace file %q: %w", cfg.TraceFile, err)
		}
		if err := trace.Start(f); err != nil {
			_ = f.Close()
			_ = p.Stop()
			return nil, fmt.Errorf("could not start trace %q: %w", cfg.TraceFile, err)
		}
		p.traceFile = f
	}

	return p, nil
}

func newDecisionLogger(path string) (*decisionLogger, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("could not create decision log %q: %w", path, err)
	}
	return &decisionLogger{
		file:   f,
		writer: bufio.NewWriter(f),
	}, nil
}

func (l *decisionLogger) Log(itemized, path string, isDir bool) error {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if isDir && path != "" && path[len(path)-1] != '/' {
		path += "/"
	}
	if _, err := fmt.Fprintf(l.writer, "%s %s\n", itemized, path); err != nil {
		return fmt.Errorf("could not write decision log: %w", err)
	}
	return nil
}

func (l *decisionLogger) Close() error {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	var errs []error
	if l.writer != nil {
		if err := l.writer.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("could not flush decision log %q: %w", l.file.Name(), err))
		}
		l.writer = nil
	}
	if l.file != nil {
		if err := l.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("could not close decision log %q: %w", l.file.Name(), err))
		}
		l.file = nil
	}
	return errors.Join(errs...)
}

func (ctx CopyContext) logDecision(itemized, path string, isDir bool) error {
	if ctx.DecisionLog == nil {
		return nil
	}
	return ctx.DecisionLog.Log(itemized, path, isDir)
}

func (p *profiler) Stop() error {
	if p == nil {
		return nil
	}

	var errs []error

	if p.traceFile != nil {
		trace.Stop()
		if err := p.traceFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("could not close trace file %q: %w", p.cfg.TraceFile, err))
		} else {
			p.logf("[profile] wrote trace profile to %q", p.cfg.TraceFile)
		}
		p.traceFile = nil
	}

	if p.cpuFile != nil {
		pprof.StopCPUProfile()
		if err := p.cpuFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("could not close CPU profile %q: %w", p.cfg.CPUProfile, err))
		} else {
			p.logf("[profile] wrote CPU profile to %q", p.cfg.CPUProfile)
		}
		p.cpuFile = nil
	}

	if p.cfg.MemProfile != "" {
		runtime.GC()
		f, err := os.Create(p.cfg.MemProfile)
		if err != nil {
			errs = append(errs, fmt.Errorf("could not create memory profile %q: %w", p.cfg.MemProfile, err))
		} else {
			if err := pprof.WriteHeapProfile(f); err != nil {
				errs = append(errs, fmt.Errorf("could not write memory profile %q: %w", p.cfg.MemProfile, err))
			} else {
				p.logf("[profile] wrote memory profile to %q", p.cfg.MemProfile)
			}
			if err := f.Close(); err != nil {
				errs = append(errs, fmt.Errorf("could not close memory profile %q: %w", p.cfg.MemProfile, err))
			}
		}
	}

	if err := p.writeNamedProfile("block", p.cfg.BlockProfile); err != nil {
		errs = append(errs, err)
	}
	if err := p.writeNamedProfile("mutex", p.cfg.MutexProfile); err != nil {
		errs = append(errs, err)
	}
	if err := p.writeNamedProfile("goroutine", p.cfg.GoroutineProfile); err != nil {
		errs = append(errs, err)
	}

	runtime.SetBlockProfileRate(0)
	runtime.SetMutexProfileFraction(0)

	return errors.Join(errs...)
}

func (p *profiler) writeNamedProfile(name, path string) error {
	if path == "" {
		return nil
	}

	profile := pprof.Lookup(name)
	if profile == nil {
		return fmt.Errorf("profile %q is not available", name)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("could not create %s profile %q: %w", name, path, err)
	}
	defer func() {
		_ = f.Close()
	}()

	if err := profile.WriteTo(f, 0); err != nil {
		return fmt.Errorf("could not write %s profile %q: %w", name, path, err)
	}
	p.logf("[profile] wrote %s profile to %q", name, path)
	return nil
}

func (p *profiler) logf(format string, args ...any) {
	if p != nil && p.printer != nil {
		p.printer.Logf(format, args...)
		return
	}
	log.Printf(format, args...)
}

func printCopySummaryWithStatus(ctx CopyContext, start time.Time, stats *AvroStats, errorCount int64, interrupted bool) {
	elapsed := time.Since(start)
	dirs := atomic.LoadInt64(&stats.TotalDirs)
	files := atomic.LoadInt64(&stats.TotalFiles)
	others := atomic.LoadInt64(&stats.TotalOthers)
	rate := float64(dirs+files) / elapsed.Seconds()

	status := "Copie complete"
	if interrupted {
		status = "Copie interrompue"
	}

	fmt.Printf("\n%s\n\n", status)
	fmt.Printf("%-18s%s\n", "Source Path", ctx.SourceDir)
	fmt.Printf("%-18s%s\n", "Destination Path", ctx.DestinationDir)
	fmt.Printf("%-18s%s • %s obj/s\n\n", "Duration", compactDuration(elapsed), humanSI(int64(rate)))
	fmt.Printf("%-18s%s dirs • %s files • %s others\n", "Entries",
		humanSI(dirs),
		humanSI(files),
		humanSI(others),
	)
	fmt.Printf("%-18s%s\n", "Total size", humanBytes(atomic.LoadInt64(&stats.TotalBytes)))
	fmt.Printf("%-18s%d\n\n", "Errors", errorCount)
}

func printCopySummary(ctx CopyContext, start time.Time, stats *AvroStats, errorCount int64) {
	printCopySummaryWithStatus(ctx, start, stats, errorCount, false)
}

func processRecord(ctx CopyContext, state *workerState, record avro.Record, stats *AvroStats) error {
	switch record.Type {
	case avro.TypeDir:
		return processDirectoryRecord(ctx, state, record, stats)
	case avro.TypeFile:
		return processFileRecord(ctx, state, record, stats)
	default:
		return processOtherRecord(ctx, record, stats)
	}
}

func processDirectoryRecord(ctx CopyContext, state *workerState, record avro.Record, stats *AvroStats) error {
	sourcePath := ctx.sourcePathFor(record.Path)
	destinationPath := ctx.destinationPathFor(record.Path)

	sourceInfo, err := os.Lstat(sourcePath)
	if err != nil {
		return fmt.Errorf("could not stat source directory %q: %w", sourcePath, err)
	}
	if !sourceInfo.IsDir() {
		return fmt.Errorf("source path %q is not a directory", sourcePath)
	}

	metadata, err := readSourceDirectoryMetadata(sourcePath, sourceInfo, record)
	if err != nil {
		return err
	}

	destinationInfo, err := os.Stat(destinationPath)
	switch {
	case err == nil:
		if !destinationInfo.IsDir() {
			return fmt.Errorf("destination path %q is not a directory", destinationPath)
		}
		matches, err := directoryMetadataMatches(destinationPath, destinationInfo, metadata, record)
		if err != nil {
			return err
		}
		if matches {
			if stats != nil {
				atomic.AddInt64(&stats.TotalDirs, 1)
			}
			return ctx.logDecision(".d.........", record.Path, true)
		}
	case os.IsNotExist(err):
		if err := ensureParentDirectory(state, destinationPath); err != nil {
			return err
		}
		if err := os.Mkdir(destinationPath, metadata.Mode.Perm()); err != nil {
			if !os.IsExist(err) {
				return fmt.Errorf("could not create destination directory %q: %w", destinationPath, err)
			}
		}
		if err := applyDirectoryMetadata(destinationPath, metadata); err != nil {
			return err
		}
		if stats != nil {
			atomic.AddInt64(&stats.TotalDirs, 1)
		}
		return ctx.logDecision(">d+++++++++", record.Path, true)
	default:
		return fmt.Errorf("could not stat destination directory %q: %w", destinationPath, err)
	}

	if err := ensureParentDirectory(state, destinationPath); err != nil {
		return err
	}

	if err := os.Mkdir(destinationPath, metadata.Mode.Perm()); err != nil {
		if !os.IsExist(err) {
			return fmt.Errorf("could not create destination directory %q: %w", destinationPath, err)
		}
	}

	if err := applyDirectoryMetadata(destinationPath, metadata); err != nil {
		return err
	}

	if stats != nil {
		atomic.AddInt64(&stats.TotalDirs, 1)
	}
	return ctx.logDecision(">d...pogua.", record.Path, true)
}

func ensureParentDirectory(state *workerState, destinationPath string) error {
	parentPath := filepath.Dir(destinationPath)
	if parentPath == "." || parentPath == "" {
		return nil
	}
	if state != nil && state.ensuredParents.has(parentPath) {
		return nil
	}
	if err := os.MkdirAll(parentPath, 0o755); err != nil {
		return fmt.Errorf("could not create destination parent directory %q: %w", parentPath, err)
	}
	if state != nil {
		state.ensuredParents.add(parentPath)
	}
	return nil
}

func processFileRecord(ctx CopyContext, state *workerState, record avro.Record, stats *AvroStats) error {
	sourcePath := ctx.sourcePathFor(record.Path)
	destinationPath := ctx.destinationPathFor(record.Path)

	if err := ensureParentDirectory(state, destinationPath); err != nil {
		return err
	}

	destinationInfo, err := os.Stat(destinationPath)
	switch {
	case os.IsNotExist(err):
		if err := copyFileData(sourcePath, destinationPath); err != nil {
			return err
		}
		metadata, err := readSourceFileMetadata(sourcePath, record)
		if err != nil {
			return err
		}
		if err := applyFileMetadata(destinationPath, metadata); err != nil {
			return err
		}
		if stats != nil {
			atomic.AddInt64(&stats.TotalFiles, 1)
			atomic.AddInt64(&stats.TotalBytes, record.Size)
		}
		return ctx.logDecision(">f+++++++++", record.Path, false)
	default:
		if err != nil {
			return fmt.Errorf("could not stat destination file %q: %w", destinationPath, err)
		}
	}

	sameSize := destinationInfo.Size() == record.Size
	sameMTime := destinationInfo.ModTime().UnixNano() == record.MTimeUnixNs
	if !sameSize || !sameMTime {
		if err := copyFileData(sourcePath, destinationPath); err != nil {
			return err
		}
		metadata, err := readSourceFileMetadata(sourcePath, record)
		if err != nil {
			return err
		}
		if err := applyFileMetadata(destinationPath, metadata); err != nil {
			return err
		}
		if stats != nil {
			atomic.AddInt64(&stats.TotalFiles, 1)
			atomic.AddInt64(&stats.TotalBytes, record.Size)
		}
		return ctx.logDecision(fileChangeCode(sameSize, sameMTime), record.Path, false)
	}

	destinationCTime, err := ctimeUnixNs(destinationInfo)
	if err != nil {
		return err
	}
	if destinationCTime >= record.CTimeUnixNs {
		if stats != nil {
			atomic.AddInt64(&stats.TotalFiles, 1)
			atomic.AddInt64(&stats.TotalBytes, record.Size)
		}
		return ctx.logDecision(".f.........", record.Path, false)
	}

	metadata, err := readSourceFileMetadata(sourcePath, record)
	if err != nil {
		return err
	}
	if err := applyFileMetadata(destinationPath, metadata); err != nil {
		return err
	}
	if stats != nil {
		atomic.AddInt64(&stats.TotalFiles, 1)
		atomic.AddInt64(&stats.TotalBytes, record.Size)
	}
	return ctx.logDecision(">f...pogua.", record.Path, false)
}

func processOtherRecord(ctx CopyContext, record avro.Record, stats *AvroStats) error {
	destinationPath := ctx.destinationPathFor(record.Path)

	_, err := os.Stat(destinationPath)
	switch {
	case err == nil:
		if stats != nil {
			atomic.AddInt64(&stats.TotalOthers, 1)
		}
		return nil
	case os.IsNotExist(err):
		if stats != nil {
			atomic.AddInt64(&stats.TotalOthers, 1)
		}
		return nil
	default:
		return fmt.Errorf("could not stat destination path %q: %w", destinationPath, err)
	}
}

func fileChangeCode(sameSize, sameMTime bool) string {
	switch {
	case !sameSize && !sameMTime:
		return ">f.st......"
	case !sameSize:
		return ">f.s......."
	case !sameMTime:
		return ">f..t......"
	default:
		return ".f........."
	}
}

func printCopySummaryLegacy(ctx CopyContext, start time.Time, stats *AvroStats, errorCount int64) {
	elapsed := time.Since(start)
	dirs := atomic.LoadInt64(&stats.TotalDirs)
	files := atomic.LoadInt64(&stats.TotalFiles)
	others := atomic.LoadInt64(&stats.TotalOthers)
	rate := float64(dirs+files) / elapsed.Seconds()

	fmt.Printf("\nCopie complete\n\n")
	fmt.Printf("%-18s%s\n", "Source Path", ctx.SourceDir)
	fmt.Printf("%-18s%s\n", "Destination Path", ctx.DestinationDir)
	fmt.Printf("%-18s%s • %s obj/s\n\n", "Duration", compactDuration(elapsed), humanSI(int64(rate)))
	fmt.Printf("%-18s%s dirs • %s files • %s others\n", "Entries",
		humanSI(dirs),
		humanSI(files),
		humanSI(others),
	)
	fmt.Printf("%-18s%s\n", "Total size", humanBytes(atomic.LoadInt64(&stats.TotalBytes)))
	fmt.Printf("%-18s%d\n\n", "Errors", errorCount)
}

func applyDirectoryMetadata(destinationPath string, metadata DirectoryMetadata) error {
	mode := metadata.Mode.Perm() | (metadata.Mode & (os.ModeSetuid | os.ModeSetgid | os.ModeSticky))
	if err := os.Chmod(destinationPath, mode); err != nil {
		return fmt.Errorf("could not chmod destination directory %q: %w", destinationPath, err)
	}

	if metadata.HasOwnership {
		if err := os.Chown(destinationPath, metadata.UID, metadata.GID); err != nil {
			return fmt.Errorf("could not chown destination directory %q: %w", destinationPath, err)
		}
	}

	if err := copyPathXattrs(destinationPath, metadata.Xattrs); err != nil {
		return err
	}

	if !metadata.Atime.IsZero() || !metadata.MTime.IsZero() {
		if err := os.Chtimes(destinationPath, metadata.Atime, metadata.MTime); err != nil {
			return fmt.Errorf("could not chtimes destination directory %q: %w", destinationPath, err)
		}
	}

	return nil
}

func directoryMetadataMatches(destinationPath string, destinationInfo os.FileInfo, metadata DirectoryMetadata, record avro.Record) (bool, error) {
	expectedMode := metadata.Mode.Perm() | (metadata.Mode & (os.ModeSetuid | os.ModeSetgid | os.ModeSticky))
	actualMode := destinationInfo.Mode().Perm() | (destinationInfo.Mode() & (os.ModeSetuid | os.ModeSetgid | os.ModeSticky))
	if actualMode != expectedMode {
		return false, nil
	}

	if metadata.HasOwnership {
		stat, ok := destinationInfo.Sys().(*syscall.Stat_t)
		if !ok {
			return false, fmt.Errorf("unexpected stat type for %q", destinationPath)
		}
		if int(stat.Uid) != metadata.UID || int(stat.Gid) != metadata.GID {
			return false, nil
		}
	}

	if destinationInfo.ModTime().UnixNano() != metadata.MTime.UnixNano() {
		return false, nil
	}

	destinationCTime, err := ctimeUnixNs(destinationInfo)
	if err != nil {
		return false, err
	}
	if destinationCTime < record.CTimeUnixNs {
		return false, nil
	}

	actualXattrs, err := readPathXattrs(destinationPath)
	if err != nil {
		return false, err
	}
	if !sameXattrs(actualXattrs, metadata.Xattrs) {
		return false, nil
	}

	return true, nil
}

func sameXattrs(left, right map[string][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for name, leftValue := range left {
		rightValue, ok := right[name]
		if !ok {
			return false
		}
		if !bytes.Equal(leftValue, rightValue) {
			return false
		}
	}
	return true
}

func formatAvroSummary(stats *AvroStats) string {
	if stats == nil {
		return ""
	}

	return fmt.Sprintf("%s dirs • %s files • %s",
		humanSI(atomic.LoadInt64(&stats.TotalDirs)),
		humanSI(atomic.LoadInt64(&stats.TotalFiles)),
		humanBytes(atomic.LoadInt64(&stats.TotalBytes)),
	)
}

func reportAvroProgress(ctx context.Context, printer *stderrPrinter, stats *AvroStats, start time.Time) {
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
			dirs := atomic.LoadInt64(&stats.TotalDirs)
			files := atomic.LoadInt64(&stats.TotalFiles)
			total := dirs + files
			rate := float64(total-prevObjects) / interval.Seconds()
			prevObjects = total

			printer.PrintProgress(formatAvroProgress(stats, time.Since(start), int64(rate)))
		}
	}
}

func formatAvroProgress(stats *AvroStats, elapsed time.Duration, rate int64) string {
	if stats == nil {
		return fmt.Sprintf("[%s]", compactDuration(elapsed))
	}

	dirs := atomic.LoadInt64(&stats.TotalDirs)
	files := atomic.LoadInt64(&stats.TotalFiles)
	size := atomic.LoadInt64(&stats.TotalBytes)

	return fmt.Sprintf("[%s] %s dirs • %s files • %s obj/s • %s",
		compactDuration(elapsed),
		humanSI(dirs),
		humanSI(files),
		humanSI(rate),
		humanBytes(size),
	)
}

func copyFileData(sourcePath, destinationPath string) error {
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("could not open source file %q: %w", sourcePath, err)
	}
	defer sourceFile.Close()

	destinationFile, err := os.Create(destinationPath)
	if err != nil {
		return fmt.Errorf("could not create destination file %q: %w", destinationPath, err)
	}
	defer func() {
		_ = destinationFile.Close()
	}()

	if _, err := io.Copy(destinationFile, sourceFile); err != nil {
		return fmt.Errorf("could not copy file %q to %q: %w", sourcePath, destinationPath, err)
	}
	if err := destinationFile.Close(); err != nil {
		return fmt.Errorf("could not close destination file %q: %w", destinationPath, err)
	}
	return nil
}

func applyFileMetadata(destinationPath string, metadata DirectoryMetadata) error {
	mode := metadata.Mode.Perm() | (metadata.Mode & (os.ModeSetuid | os.ModeSetgid | os.ModeSticky))
	if err := os.Chmod(destinationPath, mode); err != nil {
		return fmt.Errorf("could not chmod destination file %q: %w", destinationPath, err)
	}

	if metadata.HasOwnership {
		if err := os.Chown(destinationPath, metadata.UID, metadata.GID); err != nil {
			return fmt.Errorf("could not chown destination file %q: %w", destinationPath, err)
		}
	}

	if err := copyPathXattrs(destinationPath, metadata.Xattrs); err != nil {
		return err
	}

	if !metadata.Atime.IsZero() || !metadata.MTime.IsZero() {
		if err := os.Chtimes(destinationPath, metadata.Atime, metadata.MTime); err != nil {
			return fmt.Errorf("could not chtimes destination file %q: %w", destinationPath, err)
		}
	}

	return nil
}

func readSourceDirectoryMetadata(sourcePath string, sourceInfo os.FileInfo, record avro.Record) (DirectoryMetadata, error) {
	stat, ok := sourceInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return DirectoryMetadata{}, fmt.Errorf("unexpected stat type for %q", sourcePath)
	}

	xattrs, err := readPathXattrs(sourcePath)
	if err != nil {
		return DirectoryMetadata{}, err
	}

	mode := os.FileMode(record.Mode)
	if mode == 0 {
		mode = sourceInfo.Mode()
	}

	return DirectoryMetadata{
		Mode:         mode,
		UID:          int(stat.Uid),
		GID:          int(stat.Gid),
		Atime:        time.Unix(stat.Atim.Sec, stat.Atim.Nsec),
		MTime:        sourceInfo.ModTime(),
		Xattrs:       xattrs,
		HasOwnership: true,
	}, nil
}

func readSourceFileMetadata(sourcePath string, record avro.Record) (DirectoryMetadata, error) {
	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		return DirectoryMetadata{}, fmt.Errorf("could not stat source file %q: %w", sourcePath, err)
	}
	if !sourceInfo.Mode().IsRegular() {
		return DirectoryMetadata{}, fmt.Errorf("source path %q is not a regular file", sourcePath)
	}

	stat, ok := sourceInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return DirectoryMetadata{}, fmt.Errorf("unexpected stat type for %q", sourcePath)
	}

	xattrs, err := readPathXattrs(sourcePath)
	if err != nil {
		return DirectoryMetadata{}, err
	}

	mode := os.FileMode(record.Mode)
	if mode == 0 {
		mode = sourceInfo.Mode()
	}

	return DirectoryMetadata{
		Mode:         mode,
		UID:          int(stat.Uid),
		GID:          int(stat.Gid),
		Atime:        time.Unix(stat.Atim.Sec, stat.Atim.Nsec),
		MTime:        sourceInfo.ModTime(),
		Xattrs:       xattrs,
		HasOwnership: true,
	}, nil
}

func readPathXattrs(sourcePath string) (map[string][]byte, error) {
	size, err := syscall.Listxattr(sourcePath, nil)
	if err != nil {
		return nil, fmt.Errorf("could not list xattrs for %q: %w", sourcePath, err)
	}
	if size == 0 {
		return map[string][]byte{}, nil
	}

	buffer := make([]byte, size)
	size, err = syscall.Listxattr(sourcePath, buffer)
	if err != nil {
		return nil, fmt.Errorf("could not list xattrs for %q: %w", sourcePath, err)
	}

	names := splitXattrList(buffer[:size])

	xattrs := make(map[string][]byte, len(names))
	for _, name := range names {
		valueSize, err := syscall.Getxattr(sourcePath, name, nil)
		if err != nil {
			return nil, fmt.Errorf("could not read xattr %q from %q: %w", name, sourcePath, err)
		}
		value := make([]byte, valueSize)
		valueSize, err = syscall.Getxattr(sourcePath, name, value)
		if err != nil {
			return nil, fmt.Errorf("could not read xattr %q from %q: %w", name, sourcePath, err)
		}
		xattrs[name] = append([]byte(nil), value[:valueSize]...)
	}
	return xattrs, nil
}

func copyPathXattrs(destinationPath string, xattrs map[string][]byte) error {
	for name, value := range xattrs {
		if err := syscall.Setxattr(destinationPath, name, value, 0); err != nil {
			return fmt.Errorf("could not set xattr %q on %q: %w", name, destinationPath, err)
		}
	}
	return nil
}

func ctimeUnixNs(info os.FileInfo) (int64, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("unexpected stat type for %q", info.Name())
	}
	return stat.Ctim.Sec*1_000_000_000 + stat.Ctim.Nsec, nil
}

func splitXattrList(buffer []byte) []string {
	var names []string
	start := 0
	for i, b := range buffer {
		if b != 0 {
			continue
		}
		if i > start {
			names = append(names, string(buffer[start:i]))
		}
		start = i + 1
	}
	if start < len(buffer) {
		names = append(names, string(buffer[start:]))
	}
	return names
}

func recordBufferSize(workers int) int {
	bufferSize := workers * 256
	switch {
	case bufferSize < 1024:
		return 1024
	case bufferSize > 8192:
		return 8192
	default:
		return bufferSize
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
