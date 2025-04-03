package index

import (
	"path/filepath"

	"github.com/qishenonly/FastDB/data"
	ers "github.com/qishenonly/FastDB/errors"
	"go.etcd.io/bbolt"
)

var _ Indexer = (*BPlusTree)(nil)

const bPlusTreeIndexFileName = "bptree-index"

var indexBucketName = []byte("fastdb-buckte-index")

// BPlusTree B+ Tree Index
// go.etcd.io/bbolt  This is the library that encapsulates b+ tree
// Again, if you need to look at the source code for b+ trees,
// The following link is a good place to start
// https://github.com/etcd-io/bbolt
type BPlusTree struct {
	tree *bbolt.DB
}

func NewBPlusTree(dirPath string) *BPlusTree {
	bptree, err := bbolt.Open(filepath.Join(dirPath, bPlusTreeIndexFileName), 0644, nil)
	if err != nil {
		panic(ers.ErrOpenBPTreeFailed)
	}

	// Create the corresponding bucket
	// The update method can be thought of as a single transaction,
	// and all operations within the method are committed as a single transaction.
	// There is a bucket parameter in the transaction,
	// which can be interpreted as partitioning the data.
	// After creating a bucket, a bucket is returned.
	// The returned bucket can be used to Put, Get and other methods.
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic(ers.ErrCreateBucketFailed)
	}

	return &BPlusTree{
		tree: bptree,
	}
}

func (bptree *BPlusTree) Put(key []byte, pst *data.LogRecordPst) bool {
	if err := bptree.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		// The two arguments to the Put method are required to be byte arrays
		return bucket.Put(key, data.EncodeLogRecordPst(pst))
	}); err != nil {
		panic(ers.ErrPutValueFailed)
	}
	return true
}

func (bptree *BPlusTree) Get(key []byte) *data.LogRecordPst {
	var pst *data.LogRecordPst
	// The view method allows only reads, not inserts and deletes.
	if err := bptree.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pst = data.DecodeLogRecordPst(value)
		}
		return nil
	}); err != nil {
		panic(ers.ErrGetValueFailed)
	}
	return pst
}

func (bptree *BPlusTree) Delete(key []byte) bool {
	//TODO implement me
	panic("implement me")
}

func (bptree *BPlusTree) Size() int {
	//TODO implement me
	panic("implement me")
}

func (bptree *BPlusTree) Iterator(reverse bool) Iterator {
	//TODO implement me
	panic("implement me")
}
