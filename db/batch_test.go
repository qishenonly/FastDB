package db

import (
	"os"
	"testing"

	ers "github.com/qishenonly/FastDB/errors"
	"github.com/qishenonly/FastDB/utils"
	"github.com/stretchr/testify/assert"
)

func TestDB_WriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "fastdb-batch-1")
	opts.DirPath = dir
	db, err := NewFastDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写数据之后不提交
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ers.ErrKeyNotFound, err)

	// 正常提交数据
	err = wb.Commit()
	assert.Nil(t, err)

	val, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val)
	assert.Nil(t, err)

	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ers.ErrKeyNotFound, err)
}

func TestDB_WriteBatchRestart(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "fastdb-batch-2")
	opts.DirPath = dir
	db, err := NewFastDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(3), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := NewFastDB(opts)
	assert.Nil(t, err)

	_, err = db2.Get(utils.GetTestKey(1))
	assert.Equal(t, ers.ErrKeyNotFound, err)

	// 判断事务序列号
	assert.Equal(t, uint64(2), db.transSeqNo)
}

func TestDB_WriteBatch1(t *testing.T) {
	opts := DefaultOptions
	dir := "/tmp/batch-3"
	opts.DirPath = dir
	db, err := NewFastDB(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 批量提交中间手动停止
	wbopt := DefaultWriteBatchOptions
	wbopt.MaxBatchNum = 1000000
	wb := db.NewWriteBatch(wbopt)
	for i := 0; i < 500000; i++ {
		err = wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	err = wb.Commit()
	assert.Nil(t, err)

}
