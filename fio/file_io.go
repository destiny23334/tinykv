package fio

import "os"

type FileIO struct {
	fd *os.File // 文件描述符
}

// NewFileIOManager 新建一个FileIO管理器，参数为文件名
func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd}, nil
}

func (f *FileIO) Read(b []byte, offset int64) (int, error) {
	return f.fd.ReadAt(b, offset)
}

func (f *FileIO) Write(b []byte) (int, error) {
	return f.fd.Write(b)
}

func (f *FileIO) Sync() error {
	return f.fd.Sync()
}

func (f *FileIO) Close() error {
	return f.fd.Close()
}
