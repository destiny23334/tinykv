package index

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"tinykv/data"
)

func TestBTreePut(t *testing.T) {
	tree := NewBTree()

	// 正常值
	assert.Equal(t, true, tree.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 100}))

	// value是nil
	assert.Equal(t, true, tree.Put([]byte("world"), nil))

	// key是nil
	assert.Equal(t, true, tree.Put(nil, nil))

}

func TestBTreeGet(t *testing.T) {
	index := NewBTree()
	index.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 100})
	index.Put([]byte("你好"), &data.LogRecordPos{Fid: 2, Offset: 100})

	// 正常
	val := index.Get([]byte("hello"))
	assert.Equal(t, uint32(1), val.Fid)
	assert.Equal(t, int64(100), val.Offset)

	// 不存在的key
	val = index.Get([]byte("goujiao?"))
	assert.Nil(t, val)

	// nil
	assert.Nil(t, index.Get(nil))
}

func TestBTreeDelete(t *testing.T) {
	tree := NewBTree()
	tree.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 100})
	tree.Put([]byte("world"), &data.LogRecordPos{Fid: 2, Offset: 200})

	// 正常
	assert.True(t, tree.Delete([]byte("hello")))

	// 删除不存在
	assert.False(t, tree.Delete([]byte("hello")))

	// 删除nil
	assert.False(t, tree.Delete(nil))

}
