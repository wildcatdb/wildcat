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
	"math/rand"
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
	if len(bf.Bitset) == 0 {
		t.Errorf("Expected non-empty bitset, got empty")

	}
}

func TestAddAndContains(t *testing.T) {
	bf, err := New(1000, 0.01)
	if err != nil {
		t.Errorf("Error creating BloomFilter: %v", err)
	}

	data := []byte("testdata")

	err = bf.Add(data)
	if err != nil {
		t.Errorf("Error adding data to BloomFilter: %v", err)
	}

	if !bf.Contains(data) {
		t.Errorf("Expected BloomFilter to contain data")
	}

	nonExistentData := []byte("nonexistent")
	if bf.Contains(nonExistentData) {
		t.Errorf("Expected BloomFilter to not contain non-existent data")
	}
}

func TestCollisionRate(t *testing.T) {
	// Test parameters
	expectedItems := uint(10000)
	falsePositiveRate := 0.01 // 1% expected false positive rate

	// Create a new Bloom filter
	bf, err := New(expectedItems, falsePositiveRate)
	if err != nil {
		t.Fatalf("Error creating BloomFilter: %v", err)
	}

	// Generate and add unique items to the filter
	addedItems := make([][]byte, expectedItems)
	for i := uint(0); i < expectedItems; i++ {
		// Generate random 16-byte data
		data := make([]byte, 16)
		_, err := rand.Read(data)
		if err != nil {
			t.Fatalf("Error generating random data: %v", err)
		}

		// Store item for later verification
		addedItems[i] = data

		// Add to filter
		err = bf.Add(data)
		if err != nil {
			t.Fatalf("Error adding data to BloomFilter: %v", err)
		}
	}

	// Verify all added items are found (should be 100%)
	for i, item := range addedItems {
		if !bf.Contains(item) {
			t.Errorf("Added item %d not found in BloomFilter", i)
		}
	}

	// Test for false positives with new random items
	testItems := uint(100000) // Test with 10x more items for statistical significance
	falsePositives := 0

	for i := uint(0); i < testItems; i++ {
		// Generate random data that wasn't added
		data := make([]byte, 16)
		_, err := rand.Read(data)
		if err != nil {
			t.Fatalf("Error generating random test data: %v", err)
		}

		// Check if the filter falsely reports this item as present
		if bf.Contains(data) {
			falsePositives++
		}
	}

	// Calculate actual false positive rate
	actualFPR := float64(falsePositives) / float64(testItems)

	// Calculate theoretical false positive rate
	theoreticalFPR := bf.CalculateTheoreticalFPP(expectedItems)

	// Log the results
	t.Logf("Expected FP rate: %.6f", falsePositiveRate)
	t.Logf("Theoretical FP rate: %.6f", theoreticalFPR)
	t.Logf("Actual FP rate: %.6f (%d false positives out of %d tests)",
		actualFPR, falsePositives, testItems)

	// The actual rate should be reasonably close to the theoretical rate
	// Allow for some statistical variance (3x theoretical is usually acceptable)
	maxAcceptableFPR := 3.0 * theoreticalFPR

	if actualFPR > maxAcceptableFPR {
		t.Errorf("False positive rate too high: %.6f > %.6f (3x theoretical rate)",
			actualFPR, maxAcceptableFPR)
	}
}

func BenchmarkAdd(b *testing.B) {
	bf, err := New(1000, 0.01)
	if err != nil {
		b.Errorf("Error creating BloomFilter: %v", err)
	}

	data := []byte("testdata")

	for i := 0; i < b.N; i++ {
		err = bf.Add(data)
		if err != nil {
			b.Errorf("Error adding data to BloomFilter: %v", err)

		}
	}

}

func BenchmarkContains(b *testing.B) {
	bf, err := New(1000, 0.01)
	if err != nil {
		b.Errorf("Error creating BloomFilter: %v", err)
	}

	data := []byte("testdata")
	err = bf.Add(data)
	if err != nil {
		b.Errorf("Error adding data to BloomFilter: %v", err)
	}

	for i := 0; i < b.N; i++ {
		bf.Contains(data)
	}

}

func BenchmarkFalsePositiveRate(b *testing.B) {
	testCases := []struct {
		name          string
		expectedItems uint
		targetFPR     float64
	}{
		{"Small-Low-FPR", 100, 0.001},      // Small set with very low FPR
		{"Small-Medium-FPR", 100, 0.01},    // Small set with medium FPR
		{"Medium-Low-FPR", 10000, 0.001},   // Medium set with low FPR
		{"Medium-Medium-FPR", 10000, 0.01}, // Medium set with medium FPR
		{"Large-Low-FPR", 100000, 0.001},   // Large set with low FPR (memory intensive)
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Only perform test once per configuration regardless of b.N
			b.StopTimer()

			// Create filter with specified parameters
			bf, err := New(tc.expectedItems, tc.targetFPR)
			if err != nil {
				b.Fatalf("Error creating BloomFilter: %v", err)
			}

			// Add items (using 80% of expected capacity)
			itemCount := tc.expectedItems * 80 / 100
			for i := uint(0); i < itemCount; i++ {
				data := make([]byte, 16)
				_, err := rand.Read(data)
				if err != nil {
					b.Fatalf("Error generating random data: %v", err)
				}

				err = bf.Add(data)
				if err != nil {
					b.Fatalf("Error adding data: %v", err)
				}
			}

			// Test for false positives
			testCount := uint(10000) // Fixed test count regardless of b.N
			falsePositives := 0

			b.StartTimer()
			for i := uint(0); i < testCount; i++ {
				data := make([]byte, 16)
				rand.Read(data)

				if bf.Contains(data) {
					falsePositives++
				}
			}
			b.StopTimer()

			actualFPR := float64(falsePositives) / float64(testCount)
			theoreticalFPR := bf.CalculateTheoreticalFPP(itemCount)

			b.ReportMetric(actualFPR, "actual-fpr")
			b.ReportMetric(theoreticalFPR, "theoretical-fpr")
			b.ReportMetric(float64(bf.Size)/8/1024, "size-kb")
			b.ReportMetric(float64(bf.hashCount), "hash-funcs")

			// Check if actual FPR is within acceptable range
			fprRatio := actualFPR / tc.targetFPR
			b.ReportMetric(fprRatio, "fpr-ratio")
		})
	}
}
