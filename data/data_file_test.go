package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile("/tmp", 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	t.Log(dataFile1)
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile("/tmp", 0)

}

func TestDataFile_Close(t *testing.T) {

}

func TestDataFile_Sync(t *testing.T) {

}

func TestDataFile_ReadLogRecord(t *testing.T) {

}
