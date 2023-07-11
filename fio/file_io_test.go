package fio

import (
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"testing"
)

func removeFile(name string) {
	err := os.Remove(name)
	if err != nil {
		return
	}
}

func TestNewFileIOManager(t *testing.T) {
	manager, err := NewFileIOManager("/tmp/a.txt")
	defer removeFile("/tmp/a.txt")
	assert.Nil(t, err)
	assert.NotNil(t, manager)
}

func TestFileIO_Write(t *testing.T) {
	manager, err := NewFileIOManager("/tmp/a.txt")
	defer removeFile("/tmp/a.txt")
	assert.Nil(t, err)
	n, err := manager.Write([]byte("hello"))
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
}

func TestFileIO_Read(t *testing.T) {
	manager, err := NewFileIOManager("/tmp/a.txt")
	defer removeFile("/tmp/a.txt")
	assert.Nil(t, err)
	buf := make([]byte, 1024)
	n, err := manager.Read(buf, 0)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 6, n)
}

func TestFileIO_Sync(t *testing.T) {

}

func TestFileIO_Close(t *testing.T) {

}
