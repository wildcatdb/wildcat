package wildcat

import (
	"go.mongodb.org/mongo-driver/bson"
)

// serializeSSTable uses BSON to serialize the sstable metadata
func (sst *SSTable) serializeSSTable() ([]byte, error) {
	data, err := bson.Marshal(sst)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// deserializeSSTable uses BSON to deserialize the sstable metadata
func (sst *SSTable) deserializeSSTable(data []byte) error {
	err := bson.Unmarshal(data, sst)
	if err != nil {
		return err
	}

	return nil
}

// serializeTransaction uses BSON to serialize the transaction
func (txn *Txn) serializeTransaction() ([]byte, error) {
	data, err := bson.Marshal(txn)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// deserializeTransaction uses BSON to deserialize the transaction
func (txn *Txn) deserializeTransaction(data []byte) error {
	err := bson.Unmarshal(data, txn)
	if err != nil {
		return err
	}

	return nil
}

// serializeIDGeneratorState uses BSON to serialize the ID generator state
func (idgs *IDGeneratorState) serializeIDGeneratorState() ([]byte, error) {
	data, err := bson.Marshal(idgs)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// deserializeIDGeneratorState uses BSON to deserialize the ID generator state
func (idgs *IDGeneratorState) deserializeIDGeneratorState(data []byte) error {
	err := bson.Unmarshal(data, idgs)
	if err != nil {
		return err
	}

	return nil
}
