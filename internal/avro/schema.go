package avro

const objectRecordSchema = `{
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
