package index

import (
	"github.com/google/btree"
	"sync"
	"tinykv/data"
)

// BTree b树索引
type BTree struct {
	tree *btree.BTree  // 都是指针，为什么？
	lock *sync.RWMutex // btree的库说写并发是不安全的，所以需要加锁
}

// NewBTree new一个b树索引
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: &sync.RWMutex{},
	}
}

func (b *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{
		key: key,
	}
	ans := b.tree.Get(it)
	if ans == nil || ans.(*Item).pos == nil {
		return nil
	}
	return ans.(*Item).pos
}

func (b *BTree) Put(key []byte, value *data.LogRecordPos) bool {
	it := &Item{
		key: key,
		pos: value,
	}
	b.lock.Lock()
	b.tree.ReplaceOrInsert(it)
	b.lock.Unlock()
	return true
}

func (b *BTree) Delete(key []byte) bool {
	it := &Item{
		key: key,
	}
	b.lock.Lock()
	old := b.tree.Delete(it)
	b.lock.Unlock()
	if old == nil {
		return false
	}
	return true
}
