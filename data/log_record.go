package data

import (
	"encoding/binary"
)

type LogRecordType byte

const (
	LogRecordNormal = iota
	LogRecordDelete
)

// 4   + 1    + 5      + 5
// crc + type + keyLen + valueLen
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

type LogRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

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

func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(logRecord *LogRecord, header *LogRecordHeader) uint32 {
	return 0
}
