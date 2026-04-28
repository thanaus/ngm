package avro

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

type Record struct {
	Path        string
	Type        EntryType
	Size        int64
	MTimeUnixNs int64
	CTimeUnixNs int64
	Mode        int64
}
