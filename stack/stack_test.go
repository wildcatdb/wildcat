// Package stack
//
// (C) Copyright OrinDB
//
// Original Author: Alex Gaetano Padula
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
package stack

import (
	"sync"
	"testing"
)

func TestStack_PushAndPop(t *testing.T) {
	stack := &Stack{}

	// Test pushing and popping a single value
	stack.Push(1)
	if val := stack.Pop(); val != 1 {
		t.Errorf("expected 1, got %v", val)
	}

	// Test popping from an empty stack
	if val := stack.Pop(); val != nil {
		t.Errorf("expected nil, got %v", val)
	}

	// Test pushing and popping multiple values
	stack.Push(1)
	stack.Push(2)
	stack.Push(3)

	if val := stack.Pop(); val != 3 {
		t.Errorf("expected 3, got %v", val)
	}
	if val := stack.Pop(); val != 2 {
		t.Errorf("expected 2, got %v", val)
	}
	if val := stack.Pop(); val != 1 {
		t.Errorf("expected 1, got %v", val)
	}
}

func TestStack_ConcurrentPushAndPop(t *testing.T) {
	stack := &Stack{}
	wg := sync.WaitGroup{}
	numGoroutines := 10
	numValues := 100

	// Concurrently push values onto the stack
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numValues; j++ {
				stack.Push(id*numValues + j)
			}
		}(i)
	}
	wg.Wait()

	// Concurrently pop values from the stack
	wg.Add(numGoroutines)
	results := make(chan interface{}, numGoroutines*numValues)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for {
				val := stack.Pop()
				if val == nil {
					return
				}
				results <- val
			}
		}()
	}
	wg.Wait()
	close(results)

	// Verify the number of popped values
	poppedValues := make(map[interface{}]bool)
	for val := range results {
		poppedValues[val] = true
	}
	if len(poppedValues) != numGoroutines*numValues {
		t.Errorf("expected %d unique values, got %d", numGoroutines*numValues, len(poppedValues))
	}
}
