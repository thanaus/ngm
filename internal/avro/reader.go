package avro

import (
	"context"
	"fmt"
	"os"

	goavro "github.com/linkedin/goavro/v2"
)

func ReadRecords(ctx context.Context, path string, records chan<- Record) error {
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
		if ctx.Err() != nil {
			return ctx.Err()
		}

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

		recordValue := Record{
			Path:        pathValue,
			Type:        EntryType(entryTypeValue),
			Size:        sizeValue,
			MTimeUnixNs: mtimeValue,
			CTimeUnixNs: ctimeValue,
			Mode:        modeValue,
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case records <- recordValue:
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
