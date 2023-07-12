package main

type Options struct {
	DirPath      string
	DataFileSize int64
	SyncWrites   bool // 是否每次写入都持久化
}
