// Package bloomfilter
//
// (C) Copyright Starskey
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
package bloomfilter

import (
	"testing"
)

func TestNewBloomFilter(t *testing.T) {
	bf, err := New(1000, 0.01)
	if err != nil {
		t.Errorf("Error creating BloomFilter: %v", err)
	}

	if bf.Size == 0 {
		t.Errorf("Expected non-zero size, got %d", bf.Size)
	}
	if len(bf.hashFuncs) == 0 {
		t.Errorf("Expected non-zero hash count, got %d", len(bf.hashFuncs))
	}
}

func TestAddAndContains(t *testing.T) {
	bf, err := New(1000, 0.01)
	if err != nil {
		t.Errorf("Error creating BloomFilter: %v", err)
	}

	data := []byte("testdata")

	bf.Add(data)
	if !bf.Contains(data) {
		t.Errorf("Expected BloomFilter to contain data")
	}

	nonExistentData := []byte("nonexistent")
	if bf.Contains(nonExistentData) {
		t.Errorf("Expected BloomFilter to not contain non-existent data")
	}
}

func BenchmarkAdd(b *testing.B) {
	bf, err := New(1000, 0.01)
	if err != nil {
		b.Errorf("Error creating BloomFilter: %v", err)
	}

	data := []byte("testdata")

	for i := 0; i < b.N; i++ {
		bf.Add(data)
	}
}

func BenchmarkContains(b *testing.B) {
	bf, err := New(1000, 0.01)
	if err != nil {
		b.Errorf("Error creating BloomFilter: %v", err)
	}

	data := []byte("testdata")
	bf.Add(data)

	for i := 0; i < b.N; i++ {
		bf.Contains(data)
	}
}
