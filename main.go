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
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

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
	)

	rootCmd := &cobra.Command{
		Use:     "nexus-ls <directory>",
		Short:   "Scans a directory tree and reports file statistics",
		Version: "1.0.0",
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
			run(root, workers, noSize, batch, cpuProfile)
			return nil
		},
	}

	rootCmd.Flags().IntVarP(&workers,       "jobs",       "j", 4,     "Number of parallel workers")
	rootCmd.Flags().BoolVarP(&noSize,       "no-size",    "s", false, "Skip file size collection (avoids one lstat per file)")
	rootCmd.Flags().IntVarP(&batch,         "batch",      "b", 256,   "Number of entries read per ReadDir syscall (getdents64 batch size)")
	rootCmd.Flags().StringVarP(&cpuProfile, "cpuprofile", "p", "",    "Write CPU profile to this file (analyse with: go tool pprof -top <file>)")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(root string, workers int, noSize bool, batch int, cpuProfile string) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			log.Fatalf("could not create CPU profile: %v", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			log.Fatalf("could not start CPU profile: %v", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			f.Close()
		}()
	}

	start := time.Now()

	q := newDirQueue(root)
	var result ScanResult
	var wg, reporterWg sync.WaitGroup

	reporterWg.Add(1)
	go func() {
		defer reporterWg.Done()
		reportProgress(ctx, &result, start, noSize)
	}()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWorker(ctx, q, &result, noSize, batch)
		}()
	}

	wg.Wait()

	interrupted := ctx.Err() != nil

	cancel()
	reporterWg.Wait()

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

func runWorker(ctx context.Context, q *dirQueue, result *ScanResult, noSize bool, batch int) {
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
		subdirs := scanDir(ctx, dir, result, noSize, batch)
		atomic.AddInt64(&result.ScanNs, int64(time.Since(t1)))

		q.push(subdirs)
		q.done()
	}
}

func scanDir(ctx context.Context, dir string, result *ScanResult, noSize bool, batch int) []string {
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
			switch {
			case entry.IsDir():
				atomic.AddInt64(&result.TotalDirs, 1)
				subdirs = append(subdirs, prefix+entry.Name())

			case entry.Type().IsRegular():
				atomic.AddInt64(&result.TotalFiles, 1)
				if !noSize {
					t := time.Now()
					info, infoErr := entry.Info()
					atomic.AddInt64(&result.InfoNs, int64(time.Since(t)))
					if infoErr != nil {
						log.Printf("[error] info %q: %v", prefix+entry.Name(), infoErr)
						atomic.AddInt64(&result.TotalErrors, 1)
						continue
					}
					atomic.AddInt64(&result.TotalBytes, info.Size())
				}

			default:
				atomic.AddInt64(&result.TotalOthers, 1)
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
