package fio

const (
	DataFilePerm = 0644
)

type IOManager interface {
	// Read 写数据
	Read([]byte, int64) (int, error)

	// Write 读数据
	Write([]byte) (int, error)

	// Sync 持久化
	Sync() error

	// Close 关闭IO
	Close() error

	Size() (int64, error)
}

func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
