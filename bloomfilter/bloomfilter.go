// Package bloomfilter
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
package bloomfilter

import (
	"errors"
	"hash"
	"hash/fnv"
	"math"
)

// BloomFilter struct represents a Bloom filter
type BloomFilter struct {
	Bitset    []int8      // Bitset, each int8 can store 8 bits
	Size      uint        // Size of the bit array
	hashFunc1 hash.Hash64 // First hash function
	hashFunc2 hash.Hash64 // Second hash function for double hashing
	hashCount uint        // Number of hash functions
}

// New creates a new Bloom filter with an expected number of items and false positive rate
func New(expectedItems uint, falsePositiveRate float64) (*BloomFilter, error) {
	if expectedItems == 0 {
		return nil, errors.New("expectedItems must be greater than 0")
	}

	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		return nil, errors.New("falsePositiveRate must be between 0 and 1")
	}

	// Calculate optimal size and add a safety margin for low FPR cases
	size := optimalSize(expectedItems, falsePositiveRate)
	if falsePositiveRate < 0.01 {
		// Add 20% extra space for very low FPR targets
		size = uint(float64(size) * 1.2)
	}

	// Make size a prime number (or at least odd) to improve hash distribution
	size = nextOddNumber(size)

	hashCount := optimalHashCount(size, expectedItems)

	bf := &BloomFilter{
		Bitset:    make([]int8, (size+7)/8), // Allocate enough int8s to store the bits
		Size:      size,
		hashFunc1: fnv.New64a(), // FNV-1a for first hash
		hashFunc2: fnv.New64(),  // FNV for second hash (different algorithm)
		hashCount: hashCount,
	}

	return bf, nil
}

// Add adds an item to the Bloom filter
func (bf *BloomFilter) Add(data []byte) error {
	// Get the two hash values for double hashing
	h1, h2, err := bf.getTwoHashes(data)
	if err != nil {
		return err
	}

	// h_i(x) = (h1(x) + i*h2(x)) mod m
	// This produces k different hash functions from two base hashes
	m := uint64(bf.Size)
	for i := uint(0); i < bf.hashCount; i++ {
		// Ensure h2 is relatively prime to m (odd h2 with even m, or any h2 with prime m)
		// Specifically, we'll make sure h2 is not zero and add 1 if it is
		h2Val := h2
		if h2Val%m == 0 {
			h2Val++
		}

		// Calculate position using double hashing formula
		position := (h1 + uint64(i)*h2Val) % m
		bf.Bitset[position/8] |= 1 << (position % 8)
	}

	return nil
}

// Contains checks if an item might exist in the Bloom filter
func (bf *BloomFilter) Contains(data []byte) bool {
	h1, h2, err := bf.getTwoHashes(data)
	if err != nil {
		return false
	}

	// Use same double hashing scheme as Add
	m := uint64(bf.Size)
	for i := uint(0); i < bf.hashCount; i++ {
		// Ensure h2 is relatively prime to m
		h2Val := h2
		if h2Val%m == 0 {
			h2Val++
		}

		position := (h1 + uint64(i)*h2Val) % m
		if bf.Bitset[position/8]&(1<<(position%8)) == 0 {
			return false // Definitely not in set
		}
	}
	return true // Might be in set
}

// getTwoHashes computes two independent hash values for an item
func (bf *BloomFilter) getTwoHashes(data []byte) (uint64, uint64, error) {
	bf.hashFunc1.Reset()
	_, err := bf.hashFunc1.Write(data)
	if err != nil {
		return 0, 0, err
	}
	h1 := bf.hashFunc1.Sum64()

	bf.hashFunc2.Reset()
	_, err = bf.hashFunc2.Write(data)
	if err != nil {
		return 0, 0, err
	}
	h2 := bf.hashFunc2.Sum64()

	// It's possible for small data inputs, FNV hashes might be too similar..
	// Thus we add an extra mixing step if data is small
	if len(data) < 8 {
		// Mix h1 and h2 with different patterns
		h2 = h2 ^ (h1 >> 13) ^ (h1 << 37)
	}

	return h1, h2, nil
}

// optimalSize calculates the optimal size of the bit array
func optimalSize(n uint, p float64) uint {
	return uint(math.Ceil(-float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
}

// optimalHashCount calculates the optimal number of hash functions
func optimalHashCount(size uint, n uint) uint {
	return uint(math.Ceil(float64(size) / float64(n) * math.Log(2)))
}

// nextOddNumber returns the next odd number >= n
func nextOddNumber(n uint) uint {
	if n%2 == 0 {
		return n + 1
	}
	return n
}

// CalculateTheoreticalFPP returns the theoretical false positive probability
// based on the current state of the filter
func (bf *BloomFilter) CalculateTheoreticalFPP(itemsAdded uint) float64 {
	if itemsAdded == 0 {
		return 0.0
	}

	// (1 - e^(-kn/m))^k
	k := float64(bf.hashCount)
	m := float64(bf.Size)
	n := float64(itemsAdded)

	return math.Pow(1.0-math.Exp(-k*n/m), k)
}
