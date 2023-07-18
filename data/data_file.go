package data

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"tinykv/fio"
)

var (
	ErrorInvalidCRC = errors.New("invalid CRC check")
)

const DataFileNameSuffix = ".data"

type DataFile struct {
	FileId    uint32 // 文件id
	Offset    int64
	IoManager fio.IOManager
}

// 打开一个文件作为IOManager
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	// 打开文件的绝对路径
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d%s", fileId, DataFileNameSuffix))
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	// 构造DataFile
	return &DataFile{
		FileId:    fileId,
		Offset:    0,
		IoManager: ioManager,
	}, nil
}

// Sync 活跃文件变成oldFile
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Write(data []byte) error {
	n, err := df.IoManager.Write(data)
	if err != nil {
		return err
	}
	df.Offset += int64(n)
	return nil
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// ReadLogRecord 根据offset，从fileIO对应的文件里面读取LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	size, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}
	var headerSize int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > size {
		headerSize = size - offset
	}

	// 先读头部信息
	b, err := df.readNBytes(maxLogRecordHeaderSize, offset)
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := DecodeLogRecordHeader(b)
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	logRecordSize := keySize + valueSize + headerSize

	logRecord := &LogRecord{Type: header.recordType}

	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验
	crc := getLogRecordCRC(logRecord, header)
	if crc != header.crc {
		return nil, 0, ErrorInvalidCRC
	}
	return logRecord, logRecordSize, nil
}

func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IoManager.Read(b, offset)
	if err != nil {
		return nil, err
	}
	return b, nil
}
