package db

import (
	"os"
	"testing"

	"github.com/qishenonly/FastDB/utils"
	"github.com/stretchr/testify/assert"
)

// 没有任何数据的情况下进行 merge
func TestDB_Merge(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "fastdb-merge-1")
	opts.DirPath = dir
	db, err := NewFastDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Merge()
	assert.Nil(t, err)
}

// 全部都是有效的数据
func TestDB_Merge2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "fastdb-merge-2")
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DirPath = dir
	db, err := NewFastDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := NewFastDB(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.GetListKeys()
	assert.Equal(t, 50000, len(keys))

	for i := 0; i < 50000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}

}

// 全部是无效的数据
func TestDB_Merge3(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "fastdb-merge-3")
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DirPath = dir
	db, err := NewFastDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}
	for i := 0; i < 50000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := NewFastDB(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys := db2.GetListKeys()
	assert.Equal(t, 0, len(keys))
}
