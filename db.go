package main

import (
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"tinykv/data"
	"tinykv/index"
)

// DB tinykv存储实例
type DB struct {
	options    Options                   // 配置
	fileIds    []int                     // 排好序的文件id，只能用于加载索引
	lock       sync.RWMutex              // 同一时间只能有一个进程写数据库
	activeFile *data.DataFile            // 活跃的那个文件
	oldFiles   map[uint32]*data.DataFile // 旧的不活跃文件
	indexer    index.Indexer             // 内存索引
}

func (options Options) Open() (*DB, error) {
	// 检查一下配置是否合理
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	//
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.Mkdir(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化
	db := &DB{
		options:    options,
		lock:       sync.RWMutex{},
		activeFile: nil,
		oldFiles:   make(map[uint32]*data.DataFile),
	}

	// 加载数据
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 加载索引
	if err := db.loadIndexerFromDataFile(); err != nil {
		return nil, err
	}

	return db, nil
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
	record, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	if record.Type == data.LogRecordDelete { // 记录被删除了
		return nil, ErrorKeyNotFound
	}
	return record.Value, nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrorEmptyKey
	}

	// 先看内存有没有这个key
	if pos := db.indexer.Get(key); pos == nil {
		return ErrorKeyNotFound
	}

	// 再写到文件数据中
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDelete, // 不需要value
	}
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	return nil
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

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("目录为空")
	} else if options.DataFileSize <= 0 {
		return errors.New("数据小了")
	}
	return nil
}

func (db *DB) loadDataFiles() error {
	// 读出所有的文件
	dirEntry, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int // 不能用uint32，因为后面要排序
	for _, entry := range dirEntry {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			names := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(names[0])
			if err != nil {
				return err
			}
			fileIds = append(fileIds, fileId)
		}
	}

	sort.Ints(fileIds)
	db.fileIds = fileIds

	for i, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.oldFiles[uint32(fileId)] = dataFile
		}
	}
	return nil

}

func (db *DB) loadIndexerFromDataFile() error {
	// 空数据库
	if len(db.fileIds) == 0 {
		return nil
	}

	// 遍历所有文件，取出内容
	for i, fileId := range db.fileIds {
		fid := uint32(fileId)
		var dataFile *data.DataFile
		if fid == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.oldFiles[fid]
		}

		var offset int64 = 0
		for {
			record, n, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err != io.EOF {
					break
				}
				return err
			}
			logRecordPos := &data.LogRecordPos{Fid: fid, Offset: offset}
			if record.Type == data.LogRecordDelete {
				db.indexer.Delete(record.Key)
			} else {
				db.indexer.Put(record.Key, logRecordPos)
			}
			offset += n
		}

		if i == len(db.fileIds)-1 {
			db.activeFile.Offset = offset
		}
	}
	return nil
}
