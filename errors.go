package main

import "errors"

var (
	ErrorEmptyKey            = errors.New("empty key") // key是空值
	ErrorIndexerUpdateFailed = errors.New("update index failed")
	ErrorKeyNotFound         = errors.New("key not found")
	ErrorFileNotFound        = errors.New("file not found")
)
