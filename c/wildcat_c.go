// Package wildcat
//
// (C) Copyright Alex Gaetano Padula
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.mozilla.org/en/US/MPL/2.0/
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
#include <stdio.h>

typedef enum {
    SYNC_NONE = 0,
    SYNC_FULL,
    SYNC_PARTIAL
} sync_option_t;

typedef struct {
    char* directory;
    long write_buffer_size;
    int sync_option;
    long sync_interval_ns;
    int level_count;
    int level_multiplier;
    int block_manager_lru_size;
    double block_manager_lru_evict_ratio;
    double block_manager_lru_access_weight;
    int permission;
    int bloom_filter;
    int max_compaction_concurrency;
    long compaction_cooldown_ns;
    int compaction_batch_size;
    double compaction_size_ratio;
    int compaction_size_threshold;
    double compaction_score_size_weight;
    double compaction_score_count_weight;
	double compaction_size_tiered_similarity_ratio;
    long flusher_interval_ns;
    long compactor_interval_ns;
    double bloom_fpr;
    int wal_append_retry;
    long wal_append_backoff_ns;
    int sstable_btree_order;
    int stdout_logging;
    int max_compaction_concurrency;
    int txn_begin_retry;
    long txn_begin_backoff_ns;
    long txn_begin_max_backoff_ns;
    int recover_uncommitted_txns;
} wildcat_opts_t;

static void print_error(const char* msg) {
    fprintf(stderr, "WILDCAT ERROR: %s\n", msg);
    fflush(stderr);
}
*/
import "C"

import (
	"fmt"
	"github.com/wildcatdb/wildcat/v2"
	"os"
	"sync"
	"time"
	"unsafe"
)

var (
	dbMap     = sync.Map{} // map[uint64]*wildcat.DB
	dbCounter uint64
)

// Removed txnHandle struct and txnMap - no longer needed!

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

// Register a database and return its handle ID
func registerDB(db *wildcat.DB) uint64 {
	dbCounter++
	dbMap.Store(dbCounter, db)
	return dbCounter
}

// Get database by handle ID
func getDB(id uint64) *wildcat.DB {
	if val, ok := dbMap.Load(id); ok {
		return val.(*wildcat.DB)
	}
	return nil
}

// Remove database handle
func removeDB(id uint64) {
	dbMap.Delete(id)
}

// convert C options to Go Options
func fromCOptions(copts *C.wildcat_opts_t) *wildcat.Options {
	return &wildcat.Options{
		Directory:                           C.GoString(copts.directory),
		WriteBufferSize:                     int64(copts.write_buffer_size),
		SyncOption:                          wildcat.SyncOption(copts.sync_option),
		SyncInterval:                        time.Duration(copts.sync_interval_ns),
		LevelCount:                          int(copts.level_count),
		LevelMultiplier:                     int(copts.level_multiplier),
		BlockManagerLRUSize:                 int(copts.block_manager_lru_size),
		BlockManagerLRUEvictRatio:           float64(copts.block_manager_lru_evict_ratio),
		BlockManagerLRUAccesWeight:          float64(copts.block_manager_lru_access_weight),
		Permission:                          os.FileMode(copts.permission),
		BloomFilter:                         copts.bloom_filter != 0,
		MaxCompactionConcurrency:            int(copts.max_compaction_concurrency),
		CompactionCooldownPeriod:            time.Duration(copts.compaction_cooldown_ns),
		CompactionBatchSize:                 int(copts.compaction_batch_size),
		CompactionSizeRatio:                 float64(copts.compaction_size_ratio),
		CompactionSizeThreshold:             int(copts.compaction_size_threshold),
		CompactionScoreSizeWeight:           float64(copts.compaction_score_size_weight),
		CompactionScoreCountWeight:          float64(copts.compaction_score_count_weight),
		CompactionSizeTieredSimilarityRatio: float64(copts.compaction_size_tiered_similarity_ratio),
		FlusherTickerInterval:               time.Duration(copts.flusher_interval_ns),
		CompactorTickerInterval:             time.Duration(copts.compactor_interval_ns),
		BloomFilterFPR:                      float64(copts.bloom_fpr),
		WalAppendRetry:                      int(copts.wal_append_retry),
		WalAppendBackoff:                    time.Duration(copts.wal_append_backoff_ns),
		SSTableBTreeOrder:                   int(copts.sstable_btree_order),
		STDOutLogging:                       copts.stdout_logging != 0,
		MaxConcurrentTxns:                   int(copts.max_compaction_concurrency),
		TxnBeginRetry:                       int(copts.txn_begin_retry),
		TxnBeginBackoff:                     time.Duration(copts.txn_begin_backoff_ns),
		TxnBeginMaxBackoff:                  time.Duration(copts.txn_begin_max_backoff_ns),
		RecoverUncommittedTxns:              copts.recover_uncommitted_txns != 0,
	}
}

//export wildcat_open
func wildcat_open(opts *C.wildcat_opts_t) C.ulong {
	goOpts := fromCOptions(opts)
	db, err := wildcat.Open(goOpts)
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_open failed: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return 0
	}
	return C.ulong(registerDB(db))
}

//export wildcat_close
func wildcat_close(handle C.ulong) {
	if handle == 0 {
		return
	}
	db := getDB(uint64(handle))
	if db != nil {
		_ = db.Close()
		removeDB(uint64(handle))
	}
}

//export wildcat_begin_txn
func wildcat_begin_txn(handle C.ulong) C.long {
	db := getDB(uint64(handle))
	if db == nil {
		return -1
	}
	txn, err := db.Begin()
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_begin_txn failed: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return -1

	}
	return C.long(txn.Id)
}

//export wildcat_txn_put
func wildcat_txn_put(handle C.ulong, txnId C.long, key *C.char, val *C.char) C.int {
	db := getDB(uint64(handle))
	if db == nil {
		return -1
	}

	txn, err := db.GetTxn(int64(txnId))
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_txn_put: transaction not found: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return -1
	}

	err = txn.Put(C.GoBytes(unsafe.Pointer(key), C.int(C.strlen(key))),
		C.GoBytes(unsafe.Pointer(val), C.int(C.strlen(val))))
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_txn_put failed: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return -1
	}
	return 0
}

//export wildcat_txn_get
func wildcat_txn_get(handle C.ulong, txnId C.long, key *C.char) *C.char {
	db := getDB(uint64(handle))
	if db == nil {
		return nil
	}

	txn, err := db.GetTxn(int64(txnId))
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_txn_get: transaction not found: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return nil
	}

	val, err := txn.Get(C.GoBytes(unsafe.Pointer(key), C.int(C.strlen(key))))
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_txn_get failed: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return nil
	}
	return C.CString(string(val))
}

//export wildcat_txn_delete
func wildcat_txn_delete(handle C.ulong, txnId C.long, key *C.char) C.int {
	db := getDB(uint64(handle))
	if db == nil {
		return -1
	}

	txn, err := db.GetTxn(int64(txnId))
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_txn_delete: transaction not found: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return -1
	}

	err = txn.Delete(C.GoBytes(unsafe.Pointer(key), C.int(C.strlen(key))))
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_txn_delete failed: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return -1
	}
	return 0
}

//export wildcat_txn_commit
func wildcat_txn_commit(handle C.ulong, txnId C.long) C.int {
	db := getDB(uint64(handle))
	if db == nil {
		return -1
	}

	txn, err := db.GetTxn(int64(txnId))
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_txn_commit: transaction not found: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return -1
	}

	return boolToInt(txn.Commit() == nil)
}

//export wildcat_txn_rollback
func wildcat_txn_rollback(handle C.ulong, txnId C.long) C.int {
	db := getDB(uint64(handle))
	if db == nil {
		return -1
	}

	txn, err := db.GetTxn(int64(txnId))
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_txn_rollback: transaction not found: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return -1
	}

	return boolToInt(txn.Rollback() == nil)
}

//export wildcat_txn_free
func wildcat_txn_free(handle C.ulong, txnId C.long) {
	// The database handles transaction cleanup internally when
	// commit/rollback is called, so this is essentially a no-op
	// but we keep it for API compatibility..
}

//export wildcat_txn_new_iterator
func wildcat_txn_new_iterator(handle C.ulong, txnId C.long, asc C.int) C.ulong {
	db := getDB(uint64(handle))
	if db == nil {
		return 0
	}

	txn, err := db.GetTxn(int64(txnId))
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_txn_new_iterator: transaction not found: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return 0
	}

	iter, err := txn.NewIterator(asc != 0)
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_txn_new_iterator failed: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return 0
	}
	return C.ulong(registerIterator(iter))
}

//export wildcat_txn_new_range_iterator
func wildcat_txn_new_range_iterator(handle C.ulong, txnId C.long, start, end *C.char, asc C.int) C.ulong {
	db := getDB(uint64(handle))
	if db == nil {
		return 0
	}

	txn, err := db.GetTxn(int64(txnId))
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_txn_new_range_iterator: transaction not found: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return 0
	}

	iter, err := txn.NewRangeIterator(
		C.GoBytes(unsafe.Pointer(start), C.int(C.strlen(start))),
		C.GoBytes(unsafe.Pointer(end), C.int(C.strlen(end))),
		asc != 0,
	)
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_txn_new_range_iterator failed: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return 0
	}
	return C.ulong(registerIterator(iter))
}

//export wildcat_txn_new_prefix_iterator
func wildcat_txn_new_prefix_iterator(handle C.ulong, txnId C.long, prefix *C.char, asc C.int) C.ulong {
	db := getDB(uint64(handle))
	if db == nil {
		return 0
	}

	txn, err := db.GetTxn(int64(txnId))
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_txn_new_prefix_iterator: transaction not found: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return 0
	}

	iter, err := txn.NewPrefixIterator(
		C.GoBytes(unsafe.Pointer(prefix), C.int(C.strlen(prefix))),
		asc != 0,
	)
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_txn_new_prefix_iterator failed: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return 0
	}
	return C.ulong(registerIterator(iter))
}

//export wildcat_stats
func wildcat_stats(handle C.ulong) *C.char {
	db := getDB(uint64(handle))
	if db == nil {
		return nil
	}
	stats := db.Stats()
	return C.CString(stats)
}

//export wildcat_force_flush
func wildcat_force_flush(handle C.ulong) C.int {
	db := getDB(uint64(handle))
	if db == nil {
		return -1
	}
	err := db.ForceFlush()
	return boolToInt(err == nil)
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

//export wildcat_sync
func wildcat_sync(handle C.ulong) C.int {
	db := getDB(uint64(handle))
	if db == nil {
		return -1
	}
	err := db.Sync()
	if err != nil {
		cMsg := C.CString(fmt.Sprintf("wildcat_sync failed: %v", err))
		C.print_error(cMsg)
		C.free(unsafe.Pointer(cMsg))
		return -1
	}
	return 0
}

func boolToInt(ok bool) C.int {
	if ok {
		return 0
	}
	return -1
}

func registerIterator(mi *wildcat.MergeIterator) uint64 {
	handle := &iteratorHandle{iter: mi}
	handle.key, handle.value, _, handle.valid = mi.Next()

	iterMu.Lock()
	defer iterMu.Unlock()
	iterCounter++
	iterMap.Store(iterCounter, handle)
	return iterCounter
}

func main() {
	// This is just a placeholder to ensure the package can compile.
	// The actual functionality is exposed via the exported C functions.
}
