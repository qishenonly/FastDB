package data

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常情况
	record1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("fastdb"),
		Type:  LogRecordNormal,
	}
	buf1, size := EncodeLogRecord(record1)
	assert.NotNil(t, buf1)
	assert.Greater(t, size, int64(5))

	// value 为空
	record2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	buf2, size2 := EncodeLogRecord(record2)
	assert.NotNil(t, buf2)
	assert.Greater(t, size2, int64(5))

	// Deleted 情况
	record3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("fastdb"),
		Type:  LogRecordDeleted,
	}
	buf3, size3 := EncodeLogRecord(record3)
	assert.NotNil(t, buf3)
	assert.Greater(t, size3, int64(5))
}

func TestDecodeLogRecord(t *testing.T) {
	headerBuf := []byte{98, 201, 3, 114, 0, 8, 10}
	header, size := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(7), size)
	assert.Equal(t, uint32(1912850786), header.crc)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.Equal(t, uint32(4), header.keySize)
	assert.Equal(t, uint32(5), header.valueSize)

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	header2, size2 := decodeLogRecordHeader(headerBuf2)
	assert.NotNil(t, header2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), header2.crc)
	assert.Equal(t, LogRecordNormal, header2.recordType)
	assert.Equal(t, uint32(4), header2.keySize)
	assert.Equal(t, uint32(0), header2.valueSize)

	headerBuf3 := []byte{13, 133, 166, 233, 1, 8, 10}
	header3, size3 := decodeLogRecordHeader(headerBuf3)
	t.Log(header3)
	assert.NotNil(t, header3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(3920004365), header3.crc)
	assert.Equal(t, LogRecordDeleted, header3.recordType)
	assert.Equal(t, uint32(4), header3.keySize)
	assert.Equal(t, uint32(5), header3.valueSize)
}
