package index

import "tinykv/data"

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte)
	Delete()
}

// Item 需要实现比较
type Item struct {
}

func (i *Item) Less(than Item) bool {
	return false
}
