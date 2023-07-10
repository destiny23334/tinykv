package index

import (
	"bytes"
	"github.com/google/btree"
	"tinykv/data"
)

// Indexer 作为一个索引所需要实现的接口
type Indexer interface {
	// Put 插入一个索引
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 获取索引对应的值，即文件日志记录
	Get(key []byte) *data.LogRecordPos

	// Delete 删除索引
	Delete(key []byte) bool
}

// Item key-value 需要实现比较
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(than btree.Item) bool {
	return bytes.Compare(i.key, than.(*Item).key) == -1
}
