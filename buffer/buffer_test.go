// Package buffer
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
package buffer

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBasicOperations(t *testing.T) {
	b, err := New(10)
	if err != nil {
		t.Fatalf("Failed to create buffer: %v", err)
	}

	slot, err := b.Add("test1")
	if err != nil {
		t.Fatalf("Failed to add item: %v", err)
	}

	value, err := b.Get(slot)
	if err != nil {
		t.Fatalf("Failed to get item: %v", err)
	}
	if value != "test1" {
		t.Errorf("Expected 'test1', got %v", value)
	}

	err = b.Update(slot, "updated")
	if err != nil {
		t.Fatalf("Failed to update item: %v", err)
	}

	value, err = b.Get(slot)
	if err != nil {
		t.Fatalf("Failed to get updated item: %v", err)
	}
	if value != "updated" {
		t.Errorf("Expected 'updated', got %v", value)
	}

	err = b.Remove(slot)
	if err != nil {
		t.Fatalf("Failed to remove item: %v", err)
	}

	_, err = b.Get(slot)
	if err == nil {
		t.Error("Expected error when getting removed item")
	}
}

func TestCapacityLimits(t *testing.T) {
	capacity := 5
	b, err := New(capacity)
	if err != nil {
		t.Fatalf("Failed to create buffer: %v", err)
	}

	slots := make([]int64, 0, capacity)

	for i := 0; i < capacity; i++ {
		slot, err := b.Add(fmt.Sprintf("item%d", i))
		if err != nil {
			t.Fatalf("Failed to add item %d: %v", i, err)
		}
		slots = append(slots, slot)
	}

	if !b.IsFull() {
		t.Error("Buffer should be full")
	}

	_, err = b.Add("overflow")
	if err == nil {
		t.Error("Expected error when adding to full buffer")
	}

	err = b.Remove(slots[0])
	if err != nil {
		t.Fatalf("Failed to remove item: %v", err)
	}

	if b.IsFull() {
		t.Error("Buffer should not be full after removal")
	}

	_, err = b.Add("new item")
	if err != nil {
		t.Fatalf("Failed to add item after removal: %v", err)
	}
}

func TestConcurrentAddRemove(t *testing.T) {
	const (
		bufferSize = 100
		goroutines = 20
		operations = 1000
	)

	b, err := New(bufferSize)
	if err != nil {
		t.Fatalf("Failed to create buffer: %v", err)
	}

	var wg sync.WaitGroup
	var addCount, removeCount int64
	var totalRetries int64

	for i := 0; i < goroutines/2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for op := 0; op < operations; op++ {
				retries := 0
				backoff := time.Microsecond

				for {
					slot, err := b.Add(fmt.Sprintf("item-%d-%d", id, op))
					if err == nil {
						atomic.AddInt64(&addCount, 1)

						time.Sleep(time.Duration(rand.Intn(100)) * time.Microsecond)

						// Remove it
						if removeErr := b.Remove(slot); removeErr == nil {
							atomic.AddInt64(&removeCount, 1)
						}
						break
					}

					retries++
					if retries > 100 {
						t.Errorf("Too many retries in goroutine %d", id)
						return
					}

					time.Sleep(backoff + time.Duration(rand.Intn(int(backoff))))
					backoff = time.Duration(float64(backoff) * 1.5)
					if backoff > time.Millisecond {
						backoff = time.Millisecond
					}

					atomic.AddInt64(&totalRetries, 1)
				}
			}
		}(i)
	}

	for i := goroutines / 2; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for op := 0; op < operations/2; op++ {
				for attempt := 0; attempt < 10; attempt++ {
					slot := int64(rand.Intn(bufferSize))
					if b.Remove(slot) == nil {
						atomic.AddInt64(&removeCount, 1)
						break
					}
					time.Sleep(time.Duration(rand.Intn(10)) * time.Microsecond)
				}
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Concurrent test completed:")
	t.Logf("  Adds: %d", addCount)
	t.Logf("  Removes: %d", removeCount)
	t.Logf("  Total retries: %d", totalRetries)
	t.Logf("  Final count: %d", b.Count())

	if addCount == 0 {
		t.Error("No items were added")
	}
	if removeCount == 0 {
		t.Error("No items were removed")
	}
}

func TestHighContentionScenario(t *testing.T) {
	const (
		bufferSize = 10 // Very small buffer for high contention
		goroutines = 50 // Many goroutines competing
		duration   = 100 * time.Millisecond
	)

	b, err := New(bufferSize)
	if err != nil {
		t.Fatalf("Failed to create buffer: %v", err)
	}

	var wg sync.WaitGroup
	var stats struct {
		adds       int64
		removes    int64
		updates    int64
		addRetries int64
		maxRetries int64
	}

	stop := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			localMaxRetries := int64(0)

			for {
				select {
				case <-stop:
					atomic.StoreInt64(&stats.maxRetries,
						maxInt64(atomic.LoadInt64(&stats.maxRetries), localMaxRetries))
					return
				default:
				}

				op := rand.Intn(3) // 0=add, 1=remove, 2=update

				switch op {
				case 0:
					retries := int64(0)
					backoff := time.Microsecond

					for {
						_, err := b.Add(fmt.Sprintf("data-%d", id))
						if err == nil {
							atomic.AddInt64(&stats.adds, 1)
							break
						}

						retries++
						localMaxRetries = maxInt64(localMaxRetries, retries)
						atomic.AddInt64(&stats.addRetries, 1)

						time.Sleep(backoff)
						backoff = time.Duration(float64(backoff) * 1.2)
						if backoff > 100*time.Microsecond {
							backoff = 100 * time.Microsecond
						}

						// Prevent infinite loops
						if retries > 1000 {
							break
						}
					}

				case 1:
					slot := int64(rand.Intn(bufferSize))
					if b.Remove(slot) == nil {
						atomic.AddInt64(&stats.removes, 1)
					}

				case 2:
					slot := int64(rand.Intn(bufferSize))
					if b.Update(slot, fmt.Sprintf("updated-%d", id)) == nil {
						atomic.AddInt64(&stats.updates, 1)
					}
				}

				if rand.Intn(10) == 0 {
					time.Sleep(time.Duration(rand.Intn(10)) * time.Microsecond)
				}
			}
		}(i)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	t.Logf("High contention test results:")
	t.Logf("  Adds: %d", stats.adds)
	t.Logf("  Removes: %d", stats.removes)
	t.Logf("  Updates: %d", stats.updates)
	t.Logf("  Add retries: %d", stats.addRetries)
	t.Logf("  Max retries by single goroutine: %d", stats.maxRetries)
	t.Logf("  Final buffer count: %d", b.Count())

	if stats.adds == 0 {
		t.Error("No successful adds under high contention")
	}
}

func TestRapidSlotReuse(t *testing.T) {
	const (
		bufferSize = 5
		iterations = 1000
	)

	b, err := New(bufferSize)
	if err != nil {
		t.Fatalf("Failed to create buffer: %v", err)
	}

	var wg sync.WaitGroup
	slotUsage := make(map[int64]int64)
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				slot, err := b.Add(fmt.Sprintf("item-%d", j))
				if err != nil {
					continue // Skip if buffer full
				}

				mu.Lock()
				slotUsage[slot]++
				mu.Unlock()

				// Immediately remove (rapid turnover)
				_ = b.Remove(slot)
			}
		}()
	}

	wg.Wait()

	t.Logf("Rapid slot reuse test:")
	for slot, count := range slotUsage {
		t.Logf("  Slot %d used %d times", slot, count)
	}

	if len(slotUsage) != bufferSize {
		t.Errorf("Expected all %d slots to be used, got %d", bufferSize, len(slotUsage))
	}
}

func BenchmarkBufferAdd(b *testing.B) {
	buff, _ := New(1000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			slot, err := buff.Add("benchmark data")
			if err == nil {
				_ = buff.Remove(slot)
			}
		}
	})
}

func BenchmarkBufferGet(b *testing.B) {
	buff, _ := New(1000)

	// Pre-populate buffer
	slots := make([]int64, 500)
	for i := range slots {
		slots[i], _ = buff.Add(fmt.Sprintf("data-%d", i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			slot := slots[rand.Intn(len(slots))]
			buff.Get(slot)
		}
	})
}

func BenchmarkBufferUpdate(b *testing.B) {
	buff, _ := New(1000)

	slots := make([]int64, 500)
	for i := range slots {
		slots[i], _ = buff.Add(fmt.Sprintf("data-%d", i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			slot := slots[rand.Intn(len(slots))]
			buff.Update(slot, "updated data")
		}
	})
}

func BenchmarkBufferMixed(b *testing.B) {
	buff, _ := New(1000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localSlots := make([]int64, 0, 10)

		for pb.Next() {
			op := rand.Intn(4)

			switch op {
			case 0:
				if slot, err := buff.Add("data"); err == nil {
					localSlots = append(localSlots, slot)
					if len(localSlots) > 10 {
						localSlots = localSlots[1:] // Keep it bounded
					}
				}
			case 1:
				if len(localSlots) > 0 {
					idx := rand.Intn(len(localSlots))
					_ = buff.Remove(localSlots[idx])
					localSlots = append(localSlots[:idx], localSlots[idx+1:]...)
				}
			case 2:
				if len(localSlots) > 0 {
					slot := localSlots[rand.Intn(len(localSlots))]
					buff.Get(slot)
				}
			case 3:
				if len(localSlots) > 0 {
					slot := localSlots[rand.Intn(len(localSlots))]
					buff.Update(slot, "updated")
				}
			}
		}
	})
}

func BenchmarkBufferContention(b *testing.B) {
	buff, _ := New(10)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			backoff := time.Nanosecond
			for retries := 0; retries < 100; retries++ {
				if slot, err := buff.Add("contention test"); err == nil {
					_ = buff.Remove(slot)
					break
				}

				time.Sleep(backoff)
				backoff = time.Duration(float64(backoff) * 1.1)
				if backoff > time.Microsecond {
					backoff = time.Microsecond
				}
			}
		}
	})
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func init() {
	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())
}
