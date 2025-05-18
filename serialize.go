// Package orindb
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
package orindb

import (
	"bytes"
	"encoding/gob"
)

// serializeSSTable uses gob to serialize the sstable metadata
func (sst *SSTable) serializeSSTable() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	// Serialize the sst
	err := encoder.Encode(sst)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// deserializeSSTable uses gob to deserialize the sstable metadata
func (sst *SSTable) deserializeSSTable(data []byte) error {
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	// Deserialize the sst
	err := decoder.Decode(sst)
	if err != nil {
		return err
	}

	return nil
}

// serializeTransaction uses gob to serialize the transaction
func (txn *Txn) serializeTransaction() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	// Serialize the transaction
	err := encoder.Encode(txn)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// deserializeTransaction uses gob to deserialize the transaction
func (txn *Txn) deserializeTransaction(data []byte) error {
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	// Deserialize the transaction
	err := decoder.Decode(txn)
	if err != nil {
		return err
	}

	return nil
}

// serializeBlockSet uses gob to serialize the block set
func (bs *BlockSet) serializeBlockSet() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	// Serialize the block set
	err := encoder.Encode(bs)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// deserializeBlockSet uses gob to deserialize the block set
func (bs *BlockSet) deserializeBlockSet(data []byte) error {
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	// Deserialize the block set
	err := decoder.Decode(bs)
	if err != nil {
		return err
	}

	return nil

}

func (idgs *IDGeneratorState) serializeIDGeneratorState() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	// Serialize the IDGeneratorState
	err := encoder.Encode(idgs)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (idgs *IDGeneratorState) deserializeIDGeneratorState(data []byte) error {
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	// Deserialize the IDGeneratorState
	err := decoder.Decode(idgs)
	if err != nil {
		return err
	}

	return nil
}
