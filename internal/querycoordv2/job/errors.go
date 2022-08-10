package job

import "errors"

var (
	// Load errors
	ErrCollectionLoaded        = errors.New("CollectionLoaded")
	ErrLoadParameterMismatched = errors.New("LoadParameterMismatched")
)
