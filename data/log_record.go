package data

type LogRecordType byte

const (
	LogRecordNormal = iota
	LogRecordDelete
)

type LogRecordPos struct {
	Fid    uint32
	Offset int64
}

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}
