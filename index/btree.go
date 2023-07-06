package index

import (
	"github.com/google/btree"
	"sync"
)

type BTree struct {
	tree *btree.BTree
	lock sync.Locker
}
