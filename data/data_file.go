package data

import "tinykv/fio"

const DataFileNameSuffix = ".data"

type DataFile struct {
	FileId    uint32 // 文件id
	Offset    int64
	IoManager fio.IOManager
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil

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

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}
