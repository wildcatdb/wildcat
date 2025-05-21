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
package wildcat

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

// Tests opening a brand new instance.  Will setup an initial WAL and memory table and disk levels.
func TestOpen(t *testing.T) {
	defer func() {
		_ = os.RemoveAll("testdb")

	}()

	// Create a log channel
	logChannel := make(chan string, 100) // Buffer size of 100 messages

	opts := &Options{
		Directory:  "testdb",
		LogChannel: logChannel,
	}

	wg := &sync.WaitGroup{}

	wg.Add(1)

	// Start a goroutine to listen to the log channel
	go func() {
		defer wg.Done()
		for msg := range logChannel {
			t.Logf("Log message: %s", msg)
		}
	}()

	// Open or create the database
	db, err := Open(opts)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	// Verify all l1 to l7 directories exist
	for i := 1; i <= 7; i++ {
		dir := fmt.Sprintf("%s/l%d", opts.Directory, i)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			t.Errorf("Directory %s does not exist", dir)
		}
	}

	_ = db.Close()

	wg.Wait()
}

func BenchmarkDBBloomFilterPartialSync(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb")
	}()

	logChannel := make(chan string, 100)
	opts := &Options{
		Directory:    "benchdb",
		LogChannel:   logChannel,
		BloomFilter:  true,
		SyncOption:   SyncPartial,
		SyncInterval: time.Nanosecond * 24,
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range logChannel {
			b.Logf("Log: %s", msg)
		}
	}()

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		_ = db.Close()
		wg.Wait()
	}()

	const ops = 100_000
	const valueSize = 256

	b.Log("Pre-filling keys...")
	for i := 0; i < ops; i++ {
		err = db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%d", i))
			val := make([]byte, valueSize)
			rand.Read(val)
			return txn.Put(key, val)
		})
		if err != nil {
			b.Fatalf("Pre-fill failed at key %d: %v", i, err)
		}
	}

	b.Run("Write", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
				val := make([]byte, valueSize)
				rand.Read(val)
				return txn.Put(key, val)
			})
			if err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})

	b.Run("Read", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
				_, err = txn.Get(key)
				return err
			})
			if err != nil {
				b.Fatalf("Read failed: %v", err)
			}
		}
	})

	b.Run("ConcurrentWrite", func(b *testing.B) {
		b.ResetTimer()
		wg = &sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				err = db.Update(func(txn *Txn) error {
					key := []byte(fmt.Sprintf("concurrent_key%d", rand.Intn(ops)))
					val := make([]byte, valueSize)
					rand.Read(val)
					return txn.Put(key, val)
				})
				if err != nil {
					b.Errorf("Concurrent write failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})

	b.Run("ConcurrentRead", func(b *testing.B) {
		b.ResetTimer()
		wg = &sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				err = db.Update(func(txn *Txn) error {
					key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
					_, err = txn.Get(key)
					return err
				})
				if err != nil {
					b.Errorf("Concurrent read failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})
}

func BenchmarkDBPartialSync(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb")
	}()

	logChannel := make(chan string, 100)
	opts := &Options{
		Directory:    "benchdb",
		LogChannel:   logChannel,
		SyncOption:   SyncPartial,
		SyncInterval: time.Nanosecond * 24,
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range logChannel {
			b.Logf("Log: %s", msg)
		}
	}()

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		_ = db.Close()
		wg.Wait()
	}()

	const ops = 100_000
	const valueSize = 256

	b.Log("Pre-filling keys...")
	for i := 0; i < ops; i++ {
		err = db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%d", i))
			val := make([]byte, valueSize)
			rand.Read(val)
			return txn.Put(key, val)
		})
		if err != nil {
			b.Fatalf("Pre-fill failed at key %d: %v", i, err)
		}
	}

	b.Run("Write", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
				val := make([]byte, valueSize)
				rand.Read(val)
				return txn.Put(key, val)
			})
			if err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})

	b.Run("Read", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
				_, err := txn.Get(key)
				return err
			})
			if err != nil {
				b.Fatalf("Read failed: %v", err)
			}
		}
	})

	b.Run("ConcurrentWrite", func(b *testing.B) {
		b.ResetTimer()
		wg = &sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				err = db.Update(func(txn *Txn) error {
					key := []byte(fmt.Sprintf("concurrent_key%d", rand.Intn(ops)))
					val := make([]byte, valueSize)
					rand.Read(val)
					return txn.Put(key, val)
				})
				if err != nil {
					b.Errorf("Concurrent write failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})

	b.Run("ConcurrentRead", func(b *testing.B) {
		b.ResetTimer()
		wg = &sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				err = db.Update(func(txn *Txn) error {
					key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
					_, err = txn.Get(key)
					return err
				})
				if err != nil {
					b.Errorf("Concurrent read failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})
}

func BenchmarkDBBloomFilter(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb")
	}()

	logChannel := make(chan string, 100)
	opts := &Options{
		Directory:   "benchdb",
		LogChannel:  logChannel,
		BloomFilter: true,
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range logChannel {
			b.Logf("Log: %s", msg)
		}
	}()

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		_ = db.Close()
		wg.Wait()
	}()

	const ops = 100_000
	const valueSize = 256

	b.Log("Pre-filling keys...")
	for i := 0; i < ops; i++ {
		err = db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%d", i))
			val := make([]byte, valueSize)
			rand.Read(val)
			return txn.Put(key, val)
		})
		if err != nil {
			b.Fatalf("Pre-fill failed at key %d: %v", i, err)
		}
	}

	b.Run("Write", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
				val := make([]byte, valueSize)
				rand.Read(val)
				return txn.Put(key, val)
			})
			if err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})

	b.Run("Read", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
				_, err := txn.Get(key)
				return err
			})
			if err != nil {
				b.Fatalf("Read failed: %v", err)
			}
		}
	})

	b.Run("ConcurrentWrite", func(b *testing.B) {
		b.ResetTimer()
		wg = &sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				err = db.Update(func(txn *Txn) error {
					key := []byte(fmt.Sprintf("concurrent_key%d", rand.Intn(ops)))
					val := make([]byte, valueSize)
					rand.Read(val)
					return txn.Put(key, val)
				})
				if err != nil {
					b.Errorf("Concurrent write failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})

	b.Run("ConcurrentRead", func(b *testing.B) {
		b.ResetTimer()
		wg = &sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				err = db.Update(func(txn *Txn) error {
					key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
					_, err = txn.Get(key)
					return err
				})
				if err != nil {
					b.Errorf("Concurrent read failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})
}

func BenchmarkManySST(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb")
	}()

	logChannel := make(chan string, 100)
	opts := &Options{
		WriteBufferSize: (1024 * 1024) * 8,
		BlockSetSize:    (1024 * 1024) * 4,
		Directory:       "benchdb",
		LogChannel:      logChannel,
		BloomFilter:     true,
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range logChannel {
			b.Logf("Log: %s", msg)
		}
	}()

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		_ = db.Close()
		wg.Wait()
	}()

	const ops = 100_000
	const valueSize = 256

	b.Log("Pre-filling keys...")
	for i := 0; i < ops; i++ {
		err = db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%d", i))
			val := make([]byte, valueSize)
			rand.Read(val)
			return txn.Put(key, val)
		})
		if err != nil {
			b.Fatalf("Pre-fill failed at key %d: %v", i, err)
		}
	}

	b.Run("Write", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
				val := make([]byte, valueSize)
				rand.Read(val)
				return txn.Put(key, val)
			})
			if err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})

	b.Run("Read", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
				_, err = txn.Get(key)
				return err
			})
			if err != nil {
				b.Fatalf("Read failed: %v", err)
			}
		}
	})

	b.Run("ConcurrentWrite", func(b *testing.B) {
		b.ResetTimer()
		wg = &sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				err = db.Update(func(txn *Txn) error {
					key := []byte(fmt.Sprintf("concurrent_key%d", rand.Intn(ops)))
					val := make([]byte, valueSize)
					rand.Read(val)
					return txn.Put(key, val)
				})
				if err != nil {
					b.Errorf("Concurrent write failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})

	b.Run("ConcurrentRead", func(b *testing.B) {
		b.ResetTimer()
		wg = &sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				err = db.Update(func(txn *Txn) error {
					key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
					_, err = txn.Get(key)
					return err
				})
				if err != nil {
					b.Errorf("Concurrent read failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})
}

func BenchmarkDB(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb")

	}()

	logChannel := make(chan string, 100)
	opts := &Options{
		Directory:  "benchdb",
		LogChannel: logChannel,
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range logChannel {
			b.Logf("Log: %s", msg)
		}
	}()

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		_ = db.Close()
		wg.Wait()
	}()

	const ops = 100_000
	const valueSize = 256

	b.Log("Pre-filling keys...")
	for i := 0; i < ops; i++ {
		err = db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%d", i))
			val := make([]byte, valueSize)
			rand.Read(val)
			return txn.Put(key, val)
		})
		if err != nil {
			b.Fatalf("Pre-fill failed at key %d: %v", i, err)
		}
	}

	b.Run("Write", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
				val := make([]byte, valueSize)
				rand.Read(val)
				return txn.Put(key, val)
			})
			if err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})

	b.Run("Read", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
				_, err := txn.Get(key)
				return err
			})
			if err != nil {
				b.Fatalf("Read failed: %v", err)
			}
		}
	})

	b.Run("ConcurrentWrite", func(b *testing.B) {
		b.ResetTimer()
		wg = &sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				err = db.Update(func(txn *Txn) error {
					key := []byte(fmt.Sprintf("concurrent_key%d", rand.Intn(ops)))
					val := make([]byte, valueSize)
					rand.Read(val)
					return txn.Put(key, val)
				})
				if err != nil {
					b.Errorf("Concurrent write failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})

	b.Run("ConcurrentRead", func(b *testing.B) {
		b.ResetTimer()
		wg = &sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				err = db.Update(func(txn *Txn) error {
					key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
					_, err = txn.Get(key)
					return err
				})
				if err != nil {
					b.Errorf("Concurrent read failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})
}

func BenchmarkDBBloomFilterFullSync(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb")
	}()

	logChannel := make(chan string, 100)
	opts := &Options{
		Directory:   "benchdb",
		LogChannel:  logChannel,
		BloomFilter: true,
		SyncOption:  SyncFull,
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range logChannel {
			b.Logf("Log: %s", msg)
		}
	}()

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		_ = db.Close()
		wg.Wait()
	}()

	const ops = 100_000
	const valueSize = 256

	b.Log("Pre-filling keys...")
	for i := 0; i < ops; i++ {
		err = db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%d", i))
			val := make([]byte, valueSize)
			rand.Read(val)
			return txn.Put(key, val)
		})
		if err != nil {
			b.Fatalf("Pre-fill failed at key %d: %v", i, err)
		}
	}

	b.Run("Write", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
				val := make([]byte, valueSize)
				rand.Read(val)
				return txn.Put(key, val)
			})
			if err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})

	b.Run("Read", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
				_, err = txn.Get(key)
				return err
			})
			if err != nil {
				b.Fatalf("Read failed: %v", err)
			}
		}
	})

	b.Run("ConcurrentWrite", func(b *testing.B) {
		b.ResetTimer()
		wg = &sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				err = db.Update(func(txn *Txn) error {
					key := []byte(fmt.Sprintf("concurrent_key%d", rand.Intn(ops)))
					val := make([]byte, valueSize)
					rand.Read(val)
					return txn.Put(key, val)
				})
				if err != nil {
					b.Errorf("Concurrent write failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})

	b.Run("ConcurrentRead", func(b *testing.B) {
		b.ResetTimer()
		wg = &sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				err = db.Update(func(txn *Txn) error {
					key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
					_, err = txn.Get(key)
					return err
				})
				if err != nil {
					b.Errorf("Concurrent read failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})
}

func BenchmarkDBSyncFull(b *testing.B) {
	defer func() {
		_ = os.RemoveAll("benchdb")
	}()

	logChannel := make(chan string, 100)
	opts := &Options{
		Directory:  "benchdb",
		LogChannel: logChannel,
		SyncOption: SyncFull,
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range logChannel {
			b.Logf("Log: %s", msg)
		}
	}()

	db, err := Open(opts)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		_ = db.Close()
		wg.Wait()
	}()

	const ops = 100_000
	const valueSize = 256

	b.Log("Pre-filling keys...")
	for i := 0; i < ops; i++ {
		err = db.Update(func(txn *Txn) error {
			key := []byte(fmt.Sprintf("key%d", i))
			val := make([]byte, valueSize)
			rand.Read(val)
			return txn.Put(key, val)
		})
		if err != nil {
			b.Fatalf("Pre-fill failed at key %d: %v", i, err)
		}
	}

	b.Run("Write", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
				val := make([]byte, valueSize)
				rand.Read(val)
				return txn.Put(key, val)
			})
			if err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
	})

	b.Run("Read", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = db.Update(func(txn *Txn) error {
				key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
				_, err = txn.Get(key)
				return err
			})
			if err != nil {
				b.Fatalf("Read failed: %v", err)
			}
		}
	})

	b.Run("ConcurrentWrite", func(b *testing.B) {
		b.ResetTimer()
		wg = &sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				err = db.Update(func(txn *Txn) error {
					key := []byte(fmt.Sprintf("concurrent_key%d", rand.Intn(ops)))
					val := make([]byte, valueSize)
					rand.Read(val)
					return txn.Put(key, val)
				})
				if err != nil {
					b.Errorf("Concurrent write failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})

	b.Run("ConcurrentRead", func(b *testing.B) {
		b.ResetTimer()
		wg = &sync.WaitGroup{}
		wg.Add(b.N)
		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				err = db.Update(func(txn *Txn) error {
					key := []byte(fmt.Sprintf("key%d", rand.Intn(ops)))
					_, err = txn.Get(key)
					return err
				})
				if err != nil {
					b.Errorf("Concurrent read failed: %v", err)
				}
			}(i)
		}
		wg.Wait()
	})
}

func TestSample(t *testing.T) {
	defer func() {
		_ = os.RemoveAll("testdb")
	}()

	logChannel := make(chan string, 100)
	opts := &Options{
		Directory:  "testdb",
		LogChannel: logChannel,
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range logChannel {
			t.Logf("Log message: %s", msg)
		}
	}()

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		_ = db.Close()
		wg.Wait()
	}()

	txn := db.Begin()
	defer txn.Commit()

	for i := 0; i < 10; i++ {
		err = txn.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		val, err := txn.Get([]byte(fmt.Sprintf("key%d", i)))
		if err != nil {
			t.Fatalf("Failed to get key: %v", err)
		}
		t.Logf("Got value: %s", val)
	}
}
