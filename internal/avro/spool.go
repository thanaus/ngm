package avro

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type ClaimedFile struct {
	SelectedName   string
	ProcessingPath string
}

func ValidateDir(path string) error {
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

func WriteDoneFile(avroDir string) error {
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

func ClaimNextIncomingFile(avroDir string) (claim *ClaimedFile, hasDone bool, err error) {
	incomingDir := filepath.Join(avroDir, "incoming")
	entries, err := os.ReadDir(incomingDir)
	if err != nil {
		return nil, false, err
	}

	var selectedName string
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

	if selectedName == "" {
		return nil, hasDone, nil
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, hasDone, fmt.Errorf("could not determine hostname: %w", err)
	}

	sourcePath := filepath.Join(incomingDir, selectedName)
	processingName := addProcessingSuffix(selectedName, hostname, os.Getpid())
	processingPath := filepath.Join(avroDir, "processing", processingName)
	if err := os.Rename(sourcePath, processingPath); err != nil {
		return nil, hasDone, fmt.Errorf("could not move %q to %q: %w", sourcePath, processingPath, err)
	}

	return &ClaimedFile{
		SelectedName:   selectedName,
		ProcessingPath: processingPath,
	}, hasDone, nil
}

func FinalizeClaim(avroDir string, claim *ClaimedFile, hadError bool) (targetLabel string, err error) {
	if claim == nil {
		return "", fmt.Errorf("claimed avro file is required")
	}

	targetDir := filepath.Join(avroDir, "done")
	targetLabel = "done"
	if hadError {
		targetDir = filepath.Join(avroDir, "error")
		targetLabel = "error"
	}

	finalPath := filepath.Join(targetDir, filepath.Base(claim.ProcessingPath))
	if err := os.Rename(claim.ProcessingPath, finalPath); err != nil {
		return "", fmt.Errorf("could not move %q to %q: %w", claim.ProcessingPath, finalPath, err)
	}
	return targetLabel, nil
}

func ensureLayout(avroDir string) error {
	tmpDir := filepath.Join(avroDir, "tmp")
	incomingDir := filepath.Join(avroDir, "incoming")
	processingDir := filepath.Join(avroDir, "processing")
	doneDir := filepath.Join(avroDir, "done")
	errorDir := filepath.Join(avroDir, "error")

	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return fmt.Errorf("could not create avro tmp directory %q: %w", tmpDir, err)
	}
	if err := os.MkdirAll(incomingDir, 0o755); err != nil {
		return fmt.Errorf("could not create avro incoming directory %q: %w", incomingDir, err)
	}
	if err := os.MkdirAll(processingDir, 0o755); err != nil {
		return fmt.Errorf("could not create avro processing directory %q: %w", processingDir, err)
	}
	if err := os.MkdirAll(doneDir, 0o755); err != nil {
		return fmt.Errorf("could not create avro done directory %q: %w", doneDir, err)
	}
	if err := os.MkdirAll(errorDir, 0o755); err != nil {
		return fmt.Errorf("could not create avro error directory %q: %w", errorDir, err)
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

func addProcessingSuffix(name, hostname string, pid int) string {
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	if ext == "" {
		return fmt.Sprintf("%s.%s.%d", base, hostname, pid)
	}
	return fmt.Sprintf("%s.%s.%d%s", base, hostname, pid, ext)
}
