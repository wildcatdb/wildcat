// Package bloomfilter
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
package bloomfilter

import (
	"errors"
	"hash"
	"hash/fnv"
	"math"
)

// BloomFilter struct represents a Bloom filter
type BloomFilter struct {
	Bitset    []int8        // Bitset, each int8 can store 8 bits
	Size      uint          // Size of the bit array
	hashFuncs []hash.Hash64 // Hash functions (can't be exported on purpose for serialization purposes..)
}

// New creates a new Bloom filter with an expected number of items and false positive rate
func New(expectedItems uint, falsePositiveRate float64) (*BloomFilter, error) {
	if expectedItems == 0 {
		return nil, errors.New("expectedItems must be greater than 0")
	}

	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		return nil, errors.New("falsePositiveRate must be between 0 and 1")
	}

	size := optimalSize(expectedItems, falsePositiveRate)
	hashCount := optimalHashCount(size, expectedItems)

	bf := &BloomFilter{
		Bitset:    make([]int8, (size+7)/8), // Allocate enough int8s to store the bits
		Size:      size,
		hashFuncs: make([]hash.Hash64, hashCount),
	}

	// Initialize hash functions with different seeds
	for i := uint(0); i < hashCount; i++ {
		bf.hashFuncs[i] = fnv.New64a()
	}

	return bf, nil
}

// Add adds an item to the Bloom filter
func (bf *BloomFilter) Add(data []byte) {
	for _, hf := range bf.hashFuncs {
		hf.Reset()
		hf.Write(data)
		hash := hf.Sum64()
		position := hash % uint64(bf.Size)
		bf.Bitset[position/8] |= 1 << (position % 8)
	}
}

// Contains checks if an item might exist in the Bloom filter
func (bf *BloomFilter) Contains(data []byte) bool {
	for _, hf := range bf.hashFuncs {
		hf.Reset()

		var err error
		_, err = hf.Write(data)
		if err != nil {
			return false
		}
		h := hf.Sum64()
		position := h % uint64(bf.Size)
		if bf.Bitset[position/8]&(1<<(position%8)) == 0 {
			return false // Definitely not in set
		}
	}
	return true // Might be in set
}

// optimalSize calculates the optimal size of the bit array
func optimalSize(n uint, p float64) uint {
	return uint(math.Ceil(-float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
}

// optimalHashCount calculates the optimal number of hash functions
func optimalHashCount(size uint, n uint) uint {
	return uint(math.Ceil(float64(size) / float64(n) * math.Log(2)))
}
