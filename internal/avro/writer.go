package avro

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	goavro "github.com/linkedin/goavro/v2"
)

const avroMaxRecordsPerFile = 1_000_000

type Exporter struct {
	path string
	Rows chan Record
	errc chan error
}

func StartExport(avroDir string, workers, batch int) (*Exporter, error) {
	if avroDir == "" {
		return nil, nil
	}
	if err := ensureLayout(avroDir); err != nil {
		return nil, err
	}

	baseName := fmt.Sprintf("scan-%s", time.Now().Format("20060102-150405"))
	incomingDir := filepath.Join(avroDir, "incoming")
	pathPattern := filepath.Join(incomingDir, baseName+"-*.avro")

	exporter := &Exporter{
		path: pathPattern,
		Rows: make(chan Record, exportBufferSize(workers, batch)),
		errc: make(chan error, 1),
	}

	tmpDir := filepath.Join(avroDir, "tmp")
	go func() {
		exporter.errc <- writeFiles(tmpDir, incomingDir, baseName, exporter.Rows)
	}()

	return exporter, nil
}

func (e *Exporter) PathPattern() string {
	if e == nil {
		return ""
	}
	return e.path
}

func (e *Exporter) Wait() error {
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

func writeFiles(tmpDir, incomingDir, baseName string, rows <-chan Record) (err error) {
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
			Schema:          objectRecordSchema,
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

	batchRows := make([]interface{}, 0, 1024)
	flush := func() error {
		pending := batchRows
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
		batchRows = batchRows[:0]
		return nil
	}

	for row := range rows {
		batchRows = append(batchRows, map[string]interface{}{
			"path":          row.Path,
			"entry_type":    int32(row.Type),
			"size":          row.Size,
			"mtime_unix_ns": row.MTimeUnixNs,
			"ctime_unix_ns": row.CTimeUnixNs,
			"mode":          row.Mode,
		})
		if len(batchRows) == cap(batchRows) {
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
