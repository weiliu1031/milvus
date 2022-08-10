package meta

import "errors"

var (
	// Store errors
	ErrStoreCollectionFailed = errors.New("StoreCollectionFailed")
	ErrStoreReplicaFailed    = errors.New("StoreReplicaFailed")
)
