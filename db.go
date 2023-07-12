package main

import (
	"sync"
	"tinykv/data"
	"tinykv/index"
)

// DB tinykv存储实例
type DB struct {
	options    Options                   // 配置
	lock       sync.RWMutex              // 同一时间只能有一个进程写数据库
	activeFile *data.DataFile            // 活跃的那个文件
	oldFiles   map[uint32]*data.DataFile // 旧的不活跃文件
	indexer    index.Indexer             // 内存索引
}

// Put 数据库中插入一条键值对
func (db *DB) Put(key []byte, value []byte) error {
	// 先判断key是否有效
	if len(key) == 0 {
		return ErrorEmptyKey
	}

	// 新建一条插入记录
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加到文件中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.indexer.Put(key, pos); !ok {
		return ErrorIndexerUpdateFailed
	}
	return nil
}

// Get 从数据库中取value
func (db *DB) Get(key []byte) ([]byte, error) {
	// 为啥读也要枷锁
	db.lock.Lock()
	defer db.lock.Unlock()

	// 先判断key是否有效
	if len(key) == 0 {
		return nil, ErrorEmptyKey
	}

	pos := db.indexer.Get(key)
	if pos == nil { // 没找到，表示不存在
		return nil, ErrorKeyNotFound
	}

	// 找到对应的文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid { // 在活跃文件里面
		dataFile = db.activeFile
	} else { // 在old文件里面
		dataFile = db.oldFiles[pos.Fid]
	}

	if dataFile == nil {
		return nil, ErrorFileNotFound
	}

	// 根据偏移量读取数据
	record, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	if record.Type == data.LogRecordDelete { // 记录被删除了
		return nil, ErrorKeyNotFound
	}
	return record.Value, nil
}

// 将插入的键值对追加到磁盘文件中
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	// 初始化
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 先编码
	encode, size := data.EncodeLogRecord(record)

	// 如果写入超过了阈值，就要分片
	if size+db.activeFile.Offset > db.options.DataFileSize {
		// 先把活跃文件写入
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		db.oldFiles[db.activeFile.FileId] = db.activeFile

		// 打开一个新的文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 再写
	if err := db.activeFile.Write(encode); err != nil {
		return nil, err
	}

	// 写完了要不要持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: db.activeFile.Offset,
	}
	return pos, nil
}

// 设置新的活跃文件
// 不是线程安全的！！！
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
