package meta

import "errors"

var (
	// Read errors
	ErrCollectionNotFound = errors.New("CollectionNotFound")
	ErrPartitionNotFound  = errors.New("PartitionNotFound")

	// Store errors
	ErrStoreCollectionFailed = errors.New("StoreCollectionFailed")
	ErrStoreReplicaFailed    = errors.New("StoreReplicaFailed")
)
