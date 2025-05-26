// Package wildcat
//
// (C) Copyright Alex Gaetano Padula
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.mozilla.org/en-US/MPL/2.0/
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

/*
#include <stdlib.h>
#include <string.h>

typedef struct {
    char* directory;
    long writeBufferSize;
    int syncOption;
    long syncIntervalNs;
    int levelCount;
    int levelMultiplier;
    int blockManagerLRUSize;
    double blockManagerLRUEvictRatio;
    double blockManagerLRUAccessWeight;
    int permission;
    int bloomFilter;
    int maxCompactionConcurrency;
    long compactionCooldownNs;
    int compactionBatchSize;
    double compactionSizeRatio;
    int compactionSizeThreshold;
    double compactionScoreSizeWeight;
    double compactionScoreCountWeight;
    long flusherIntervalNs;
    long compactorIntervalNs;
    double bloomFPR;
    int walAppendRetry;
    long walAppendBackoffNs;
    int sstableBTreeOrder;
} wildcat_opts_t;
*/
import "C"

import (
	"github.com/guycipher/wildcat"
	"os"
	"sync"
	"time"
	"unsafe"
)

// store txn info per DB
type txnHandle struct {
	db  *wildcat.DB
	txn *wildcat.Txn
}

var (
	txnMap = sync.Map{} // map[int64]*txnHandle
)

type iteratorHandle struct {
	iter  *wildcat.MergeIterator
	key   []byte
	value []byte
	valid bool
}

var (
	iterMap     = sync.Map{} // map[uint64]*iteratorHandle
	iterCounter uint64       // unique ID
	iterMu      sync.Mutex
)

// convert C options to Go Options
func fromCOptions(copts *C.wildcat_opts_t) *wildcat.Options {
	return &wildcat.Options{
		Directory:                  C.GoString(copts.directory),
		WriteBufferSize:            int64(copts.writeBufferSize),
		SyncOption:                 wildcat.SyncOption(copts.syncOption),
		SyncInterval:               time.Duration(copts.syncIntervalNs),
		LevelCount:                 int(copts.levelCount),
		LevelMultiplier:            int(copts.levelMultiplier),
		BlockManagerLRUSize:        int(copts.blockManagerLRUSize),
		BlockManagerLRUEvictRatio:  float64(copts.blockManagerLRUEvictRatio),
		BlockManagerLRUAccesWeight: float64(copts.blockManagerLRUAccessWeight),
		Permission:                 os.FileMode(copts.permission),
		BloomFilter:                copts.bloomFilter != 0,
		MaxCompactionConcurrency:   int(copts.maxCompactionConcurrency),
		CompactionCooldownPeriod:   time.Duration(copts.compactionCooldownNs),
		CompactionBatchSize:        int(copts.compactionBatchSize),
		CompactionSizeRatio:        float64(copts.compactionSizeRatio),
		CompactionSizeThreshold:    int(copts.compactionSizeThreshold),
		CompactionScoreSizeWeight:  float64(copts.compactionScoreSizeWeight),
		CompactionScoreCountWeight: float64(copts.compactionScoreCountWeight),
		FlusherTickerInterval:      time.Duration(copts.flusherIntervalNs),
		CompactorTickerInterval:    time.Duration(copts.compactorIntervalNs),
		BloomFilterFPR:             float64(copts.bloomFPR),
		WalAppendRetry:             int(copts.walAppendRetry),
		WalAppendBackoff:           time.Duration(copts.walAppendBackoffNs),
		SSTableBTreeOrder:          int(copts.sstableBTreeOrder),
	}
}

//export wildcat_open
func wildcat_open(opts *C.wildcat_opts_t) unsafe.Pointer {
	goOpts := fromCOptions(opts)
	db, err := wildcat.Open(goOpts)
	if err != nil {
		return nil
	}
	return unsafe.Pointer(db)
}

//export wildcat_close
func wildcat_close(ptr unsafe.Pointer) {
	if ptr == nil {
		return
	}
	db := (*wildcat.DB)(ptr)
	_ = db.Close()
}

//export wildcat_begin_txn
func wildcat_begin_txn(ptr unsafe.Pointer) C.long {
	db := (*wildcat.DB)(ptr)
	txn := db.Begin()
	txnMap.Store(txn.Id, &txnHandle{db: db, txn: txn})
	return C.long(txn.Id)
}

//export wildcat_txn_put
func wildcat_txn_put(txnId C.long, key *C.char, val *C.char) C.int {
	h, ok := txnMap.Load(int64(txnId))
	if !ok {
		return -1
	}
	txn := h.(*txnHandle).txn
	err := txn.Put(C.GoBytes(unsafe.Pointer(key), C.int(C.strlen(key))),
		C.GoBytes(unsafe.Pointer(val), C.int(C.strlen(val))))
	if err != nil {
		return -1
	}
	return 0
}

//export wildcat_txn_get
func wildcat_txn_get(txnId C.long, key *C.char) *C.char {
	h, ok := txnMap.Load(int64(txnId))
	if !ok {
		return nil
	}
	txn := h.(*txnHandle).txn
	val, err := txn.Get(C.GoBytes(unsafe.Pointer(key), C.int(C.strlen(key))))
	if err != nil {
		return nil
	}
	return C.CString(string(val))
}

//export wildcat_txn_commit
func wildcat_txn_commit(txnId C.long) C.int {
	h, ok := txnMap.Load(int64(txnId))
	if !ok {
		return -1
	}
	txn := h.(*txnHandle).txn
	txnMap.Delete(int64(txnId))
	return boolToInt(txn.Commit() == nil)
}

//export wildcat_txn_rollback
func wildcat_txn_rollback(txnId C.long) C.int {
	h, ok := txnMap.Load(int64(txnId))
	if !ok {
		return -1
	}
	txn := h.(*txnHandle).txn
	txnMap.Delete(int64(txnId))
	return boolToInt(txn.Rollback() == nil)
}

//export wildcat_stats
func wildcat_stats(ptr unsafe.Pointer) *C.char {
	db := (*wildcat.DB)(ptr)
	stats := db.Stats()
	return C.CString(stats)
}

//export wildcat_force_flush
func wildcat_force_flush(ptr unsafe.Pointer) C.int {
	db := (*wildcat.DB)(ptr)
	err := db.ForceFlush()
	return boolToInt(err == nil)
}

//export wildcat_txn_delete
func wildcat_txn_delete(txnId C.long, key *C.char) C.int {
	h, ok := txnMap.Load(int64(txnId))
	if !ok {
		return -1
	}
	txn := h.(*txnHandle).txn
	err := txn.Delete(C.GoBytes(unsafe.Pointer(key), C.int(C.strlen(key))))
	if err != nil {
		return -1
	}
	return 0
}

//export wildcat_txn_free
func wildcat_txn_free(txnId C.long) {
	txnMap.Delete(int64(txnId))
}

//export wildcat_txn_new_iterator
func wildcat_txn_new_iterator(txnId C.long, asc C.int) C.ulong {
	h, ok := txnMap.Load(int64(txnId))
	if !ok {
		return 0
	}
	iter, err := h.(*txnHandle).txn.NewIterator(asc != 0)
	if err != nil {
		return 0
	}
	return registerIterator(iter)
}

//export wildcat_txn_new_range_iterator
func wildcat_txn_new_range_iterator(txnId C.long, start, end *C.char, asc C.int) C.ulong {
	h, ok := txnMap.Load(int64(txnId))
	if !ok {
		return 0
	}
	iter, err := h.(*txnHandle).txn.NewRangeIterator(
		C.GoBytes(unsafe.Pointer(start), C.int(C.strlen(start))),
		C.GoBytes(unsafe.Pointer(end), C.int(C.strlen(end))),
		asc != 0,
	)
	if err != nil {
		return 0
	}
	return registerIterator(iter)
}

//export wildcat_txn_new_prefix_iterator
func wildcat_txn_new_prefix_iterator(txnId C.long, prefix *C.char, asc C.int) C.ulong {
	h, ok := txnMap.Load(int64(txnId))
	if !ok {
		return 0
	}
	iter, err := h.(*txnHandle).txn.NewPrefixIterator(
		C.GoBytes(unsafe.Pointer(prefix), C.int(C.strlen(prefix))),
		asc != 0,
	)
	if err != nil {
		return 0
	}
	return registerIterator(iter)
}

//export wildcat_txn_iterate_next
func wildcat_txn_iterate_next(id C.ulong) C.int {
	h, ok := iterMap.Load(uint64(id))
	if !ok {
		return -1
	}
	ih := h.(*iteratorHandle)
	ih.key, ih.value, _, ih.valid = ih.iter.Next()
	return boolToInt(ih.valid)
}

//export wildcat_txn_iterate_prev
func wildcat_txn_iterate_prev(id C.ulong) C.int {
	h, ok := iterMap.Load(uint64(id))
	if !ok {
		return -1
	}
	ih := h.(*iteratorHandle)
	ih.key, ih.value, _, ih.valid = ih.iter.Prev()
	return boolToInt(ih.valid)
}

//export wildcat_txn_iter_valid
func wildcat_txn_iter_valid(id C.ulong) C.int {
	h, ok := iterMap.Load(uint64(id))
	if !ok {
		return 0
	}
	return boolToInt(h.(*iteratorHandle).valid)
}

//export wildcat_iterator_key
func wildcat_iterator_key(id C.ulong) *C.char {
	h, ok := iterMap.Load(uint64(id))
	if !ok {
		return nil
	}
	return C.CString(string(h.(*iteratorHandle).key))
}

//export wildcat_iterator_value
func wildcat_iterator_value(id C.ulong) *C.char {
	h, ok := iterMap.Load(uint64(id))
	if !ok {
		return nil
	}
	return C.CString(string(h.(*iteratorHandle).value))
}

//export wildcat_iterator_free
func wildcat_iterator_free(id C.ulong) {
	iterMap.Delete(uint64(id))
}

func boolToInt(ok bool) C.int {
	if ok {
		return 0
	}
	return -1
}

func registerIterator(mi *wildcat.MergeIterator) C.ulong {
	handle := &iteratorHandle{iter: mi}
	handle.key, handle.value, _, handle.valid = mi.Next()

	iterMu.Lock()
	defer iterMu.Unlock()
	iterCounter++
	iterMap.Store(iterCounter, handle)
	return C.ulong(iterCounter)
}

func main() {
	// This is just a placeholder to ensure the package can compile.
	// The actual functionality is exposed via the exported C functions.
}
