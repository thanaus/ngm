package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/spf13/cobra"
)

type CopyRecord struct {
	Path        string
	EntryType   int32
	Size        int64
	MTimeUnixNs int64
	CTimeUnixNs int64
	Mode        int64
}

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

type CopyContext struct {
	SourceDir      string
	DestinationDir string
	AvroDir        string
	Workers        int
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
	DirsCreated int64
	DirsUpdated int64
	DirsOK      int64
	FilesCreated int64
	FilesUpdated int64
	FilesOK      int64
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
	once sync.Once
	err  error
}

func (s *workerErrorSink) set(err error) {
	if err == nil {
		return
	}
	s.once.Do(func() {
		s.err = err
	})
}

func (ctx CopyContext) sourcePathFor(recordPath string) string {
	return filepath.Join(ctx.SourceDir, filepath.FromSlash(recordPath))
}

func (ctx CopyContext) destinationPathFor(recordPath string) string {
	return filepath.Join(ctx.DestinationDir, filepath.FromSlash(recordPath))
}

func main() {
	var (
		avroDir string
		workers int
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

			return run(CopyContext{
				SourceDir:      sourceDir,
				DestinationDir: destinationDir,
				AvroDir:        avroDir,
				Workers:        workers,
			})
		},
	}

	rootCmd.Flags().IntVarP(&workers, "jobs", "j", 4, "Number of parallel workers")
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

func run(ctx CopyContext) error {
	fmt.Printf("Source      : %s\n", ctx.SourceDir)
	fmt.Printf("Destination : %s\n", ctx.DestinationDir)
	if ctx.AvroDir != "" {
		fmt.Printf("Avro dir    : %s\n", ctx.AvroDir)
		fmt.Printf("Workers     : %d\n", ctx.Workers)
		fmt.Println()
		return processIncomingAvroFiles(ctx)
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
	if path == "" {
		return nil
	}

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("could not access avro directory %q: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("avro path %q is not a directory", path)
	}

	if err := requireDir(filepath.Join(path, "tmp")); err != nil {
		return fmt.Errorf("invalid avro directory: %w", err)
	}
	if err := requireDir(filepath.Join(path, "incoming")); err != nil {
		return fmt.Errorf("invalid avro directory: %w", err)
	}
	if err := requireDir(filepath.Join(path, "processing")); err != nil {
		return fmt.Errorf("invalid avro directory: %w", err)
	}
	if err := requireDir(filepath.Join(path, "done")); err != nil {
		return fmt.Errorf("invalid avro directory: %w", err)
	}
	if err := requireDir(filepath.Join(path, "error")); err != nil {
		return fmt.Errorf("invalid avro directory: %w", err)
	}

	return nil
}

func requireDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("required directory %q is missing or inaccessible: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("required path %q is not a directory", path)
	}
	return nil
}

func processIncomingAvroFiles(ctx CopyContext) error {
	incomingDir := filepath.Join(ctx.AvroDir, "incoming")

	for {
		selectedName, hasDone, err := nextIncomingFile(incomingDir)
		if err != nil {
			return fmt.Errorf("could not read incoming directory %q: %w", incomingDir, err)
		}
		if selectedName != "" {
			if err := processOneIncomingAvroFile(ctx, selectedName); err != nil {
				return err
			}
			continue
		}
		if hasDone {
			return nil
		}
		time.Sleep(incomingPollInterval)
	}
}

func nextIncomingFile(incomingDir string) (selectedName string, hasDone bool, err error) {
	entries, err := os.ReadDir(incomingDir)
	if err != nil {
		return "", false, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if entry.Name() == ".done" {
			hasDone = true
			continue
		}
		if selectedName == "" {
			selectedName = entry.Name()
		}
	}
	return selectedName, hasDone, nil
}

func processOneIncomingAvroFile(ctx CopyContext, selectedName string) error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("could not determine hostname: %w", err)
	}

	incomingDir := filepath.Join(ctx.AvroDir, "incoming")
	processingDir := filepath.Join(ctx.AvroDir, "processing")
	doneDir := filepath.Join(ctx.AvroDir, "done")
	errorDir := filepath.Join(ctx.AvroDir, "error")

	sourcePath := filepath.Join(incomingDir, selectedName)
	processingName := addProcessingSuffix(selectedName, hostname, os.Getpid())
	processingPath := filepath.Join(processingDir, processingName)

	if err := os.Rename(sourcePath, processingPath); err != nil {
		return fmt.Errorf("could not move %q to %q: %w", sourcePath, processingPath, err)
	}

	fmt.Printf("Processing  : %s\n\n", processingPath)
	var stats AvroStats
	processErr := processAvroPaths(ctx, processingPath, &stats)
	printAvroStats(&stats)

	targetDir := doneDir
	targetLabel := "Done"
	if processErr != nil {
		targetDir = errorDir
		targetLabel = "Error"
	}

	finalPath := filepath.Join(targetDir, filepath.Base(processingPath))
	if err := os.Rename(processingPath, finalPath); err != nil {
		moveErr := fmt.Errorf("could not move %q to %q: %w", processingPath, finalPath, err)
		if processErr != nil {
			return errors.Join(processErr, moveErr)
		}
		return moveErr
	}

	fmt.Printf("\n%-12s%s\n\n", targetLabel, finalPath)
	return processErr
}

func addProcessingSuffix(name, hostname string, pid int) string {
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	if ext == "" {
		return fmt.Sprintf("%s.%s.%d", base, hostname, pid)
	}
	return fmt.Sprintf("%s.%s.%d%s", base, hostname, pid, ext)
}

func processAvroPaths(ctx CopyContext, path string, stats *AvroStats) error {
	records := make(chan CopyRecord, recordBufferSize(ctx.Workers))
	var wg sync.WaitGroup
	var workerErrors workerErrorSink

	for i := 0; i < ctx.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			copyWorker(ctx, newWorkerState(), records, stats, &workerErrors)
		}()
	}

	readErr := readAvroRecords(path, records)
	close(records)
	wg.Wait()

	if readErr != nil && workerErrors.err != nil {
		return errors.Join(readErr, workerErrors.err)
	}
	if readErr != nil {
		return readErr
	}
	return workerErrors.err
}

func copyWorker(ctx CopyContext, state *workerState, records <-chan CopyRecord, stats *AvroStats, workerErrors *workerErrorSink) {
	for record := range records {
		if err := processRecord(ctx, state, record, stats); err != nil {
			workerErrors.set(err)
		}
	}
}

func processRecord(ctx CopyContext, state *workerState, record CopyRecord, stats *AvroStats) error {
	switch EntryType(record.EntryType) {
	case TypeDir:
		return processDirectoryRecord(ctx, state, record, stats)
	case TypeFile:
		return processFileRecord(ctx, state, record, stats)
	default:
		return processOtherRecord(ctx, record)
	}
}

func processDirectoryRecord(ctx CopyContext, state *workerState, record CopyRecord, stats *AvroStats) error {
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
				atomic.AddInt64(&stats.DirsOK, 1)
			}
			return nil
		}
	case os.IsNotExist(err):
		// handled below
	default:
		return fmt.Errorf("could not stat destination directory %q: %w", destinationPath, err)
	}

	if err := ensureParentDirectory(state, destinationPath); err != nil {
		return err
	}

	created := false
	if err := os.Mkdir(destinationPath, metadata.Mode.Perm()); err != nil {
		if !os.IsExist(err) {
			return fmt.Errorf("could not create destination directory %q: %w", destinationPath, err)
		}
	} else {
		created = true
	}

	if err := applyDirectoryMetadata(destinationPath, metadata); err != nil {
		return err
	}

	if created {
		if stats != nil {
			atomic.AddInt64(&stats.DirsCreated, 1)
		}
	} else {
		if stats != nil {
			atomic.AddInt64(&stats.DirsUpdated, 1)
		}
	}
	return nil
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

func processFileRecord(ctx CopyContext, state *workerState, record CopyRecord, stats *AvroStats) error {
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
			atomic.AddInt64(&stats.FilesCreated, 1)
		}
		return nil
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
			atomic.AddInt64(&stats.FilesUpdated, 1)
		}
		return nil
	}

	destinationCTime, err := ctimeUnixNs(destinationInfo)
	if err != nil {
		return err
	}
	if destinationCTime >= record.CTimeUnixNs {
		if stats != nil {
			atomic.AddInt64(&stats.FilesOK, 1)
		}
		return nil
	}

	metadata, err := readSourceFileMetadata(sourcePath, record)
	if err != nil {
		return err
	}
	if err := applyFileMetadata(destinationPath, metadata); err != nil {
		return err
	}
	if stats != nil {
		atomic.AddInt64(&stats.FilesUpdated, 1)
	}
	return nil
}

func processOtherRecord(ctx CopyContext, record CopyRecord) error {
	destinationPath := ctx.destinationPathFor(record.Path)

	_, err := os.Stat(destinationPath)
	switch {
	case err == nil:
		return nil
	case os.IsNotExist(err):
		return nil
	default:
		return fmt.Errorf("could not stat destination path %q: %w", destinationPath, err)
	}
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

func directoryMetadataMatches(destinationPath string, destinationInfo os.FileInfo, metadata DirectoryMetadata, record CopyRecord) (bool, error) {
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

func printAvroStats(stats *AvroStats) {
	if stats == nil {
		return
	}
	fmt.Printf("\nDirectory stats\n")
	fmt.Printf("  created  %d\n", atomic.LoadInt64(&stats.DirsCreated))
	fmt.Printf("  updated  %d\n", atomic.LoadInt64(&stats.DirsUpdated))
	fmt.Printf("  ok       %d\n", atomic.LoadInt64(&stats.DirsOK))
	fmt.Printf("\nFile stats\n")
	fmt.Printf("  created  %d\n", atomic.LoadInt64(&stats.FilesCreated))
	fmt.Printf("  updated  %d\n", atomic.LoadInt64(&stats.FilesUpdated))
	fmt.Printf("  ok       %d\n\n", atomic.LoadInt64(&stats.FilesOK))
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

func readSourceDirectoryMetadata(sourcePath string, sourceInfo os.FileInfo, record CopyRecord) (DirectoryMetadata, error) {
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

func readSourceFileMetadata(sourcePath string, record CopyRecord) (DirectoryMetadata, error) {
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

func readAvroRecords(path string, records chan<- CopyRecord) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("could not open avro file %q: %w", path, err)
	}
	defer f.Close()

	reader, err := goavro.NewOCFReader(f)
	if err != nil {
		return fmt.Errorf("could not create avro reader for %q: %w", path, err)
	}

	for reader.Scan() {
		datum, err := reader.Read()
		if err != nil {
			return fmt.Errorf("could not read avro record from %q: %w", path, err)
		}

		record, ok := datum.(map[string]interface{})
		if !ok {
			return fmt.Errorf("unexpected avro record type %T", datum)
		}

		rawPath, ok := record["path"]
		if !ok {
			continue
		}

		pathValue, ok := rawPath.(string)
		if !ok {
			return fmt.Errorf("unexpected avro path type %T", rawPath)
		}

		entryTypeValue, err := getInt32Field(record, "entry_type")
		if err != nil {
			return err
		}

		sizeValue, err := getInt64Field(record, "size")
		if err != nil {
			return err
		}

		mtimeValue, err := getInt64Field(record, "mtime_unix_ns")
		if err != nil {
			return err
		}

		ctimeValue, err := getInt64Field(record, "ctime_unix_ns")
		if err != nil {
			return err
		}

		modeValue, err := getInt64Field(record, "mode")
		if err != nil {
			return err
		}

		records <- CopyRecord{
			Path:        pathValue,
			EntryType:   entryTypeValue,
			Size:        sizeValue,
			MTimeUnixNs: mtimeValue,
			CTimeUnixNs: ctimeValue,
			Mode:        modeValue,
		}
	}

	return nil
}

func getInt32Field(record map[string]interface{}, field string) (int32, error) {
	value, err := getInt64Field(record, field)
	if err != nil {
		return 0, err
	}
	return int32(value), nil
}

func getInt64Field(record map[string]interface{}, field string) (int64, error) {
	rawValue, ok := record[field]
	if !ok {
		return 0, fmt.Errorf("missing avro field %q", field)
	}

	switch value := rawValue.(type) {
	case int:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case int64:
		return value, nil
	case float32:
		return int64(value), nil
	case float64:
		return int64(value), nil
	default:
		return 0, fmt.Errorf("unexpected avro field %q type %T", field, rawValue)
	}
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
