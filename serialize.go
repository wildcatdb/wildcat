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
	"go.mongodb.org/mongo-driver/bson"
)

// serializeSSTable uses BSON to serialize the sstable metadata
func (sst *SSTable) serializeSSTable() ([]byte, error) {
	// Serialize the sst to BSON
	data, err := bson.Marshal(sst)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// deserializeSSTable uses BSON to deserialize the sstable metadata
func (sst *SSTable) deserializeSSTable(data []byte) error {
	// Deserialize the sst from BSON
	err := bson.Unmarshal(data, sst)
	if err != nil {
		return err
	}

	return nil
}

// serializeTransaction uses BSON to serialize the transaction
func (txn *Txn) serializeTransaction() ([]byte, error) {
	// Serialize the transaction to BSON
	data, err := bson.Marshal(txn)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// deserializeTransaction uses BSON to deserialize the transaction
func (txn *Txn) deserializeTransaction(data []byte) error {
	// Deserialize the transaction from BSON
	err := bson.Unmarshal(data, txn)
	if err != nil {
		return err
	}

	return nil
}

// serializeIDGeneratorState uses BSON to serialize the ID generator state
func (idgs *IDGeneratorState) serializeIDGeneratorState() ([]byte, error) {
	// Serialize the IDGeneratorState to BSON
	data, err := bson.Marshal(idgs)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// deserializeIDGeneratorState uses BSON to deserialize the ID generator state
func (idgs *IDGeneratorState) deserializeIDGeneratorState(data []byte) error {
	// Deserialize the IDGeneratorState from BSON
	err := bson.Unmarshal(data, idgs)
	if err != nil {
		return err
	}

	return nil
}
