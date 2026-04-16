// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

const (
	// DefaultFragmentRowLimit is the default row limit for splitting large files into fragments
	DefaultFragmentRowLimit = 1000000 // 1 million rows
)

// SegmentFragments maps segment ID to its fragments
type SegmentFragments map[int64][]Fragment

// FragmentIDGenerator generates sequential fragment IDs. Not safe for concurrent use.
type FragmentIDGenerator func() int64

// NewFragmentIDGenerator creates a generator that returns sequential IDs starting from start.
func NewFragmentIDGenerator(start int64) FragmentIDGenerator {
	nextFragmentID := start
	return func() int64 {
		id := nextFragmentID
		nextFragmentID++
		return id
	}
}

// SplitFileToFragments splits a large file into multiple fragments based on row count.
// If totalRows <= rowLimit, returns a single fragment covering the entire file.
// Otherwise, splits the file into multiple fragments with rowLimit rows each.
func SplitFileToFragments(
	filePath string,
	totalRows int64,
	rowLimit int64,
	fragmentIDGenerator FragmentIDGenerator,
) []Fragment {
	if totalRows <= rowLimit {
		return []Fragment{{
			FragmentID: fragmentIDGenerator(),
			FilePath:   filePath,
			StartRow:   0,
			EndRow:     totalRows,
			RowCount:   totalRows,
		}}
	}

	var fragments []Fragment
	for start := int64(0); start < totalRows; start += rowLimit {
		end := start + rowLimit
		if end > totalRows {
			end = totalRows
		}
		fragments = append(fragments, Fragment{
			FragmentID: fragmentIDGenerator(),
			FilePath:   filePath,
			StartRow:   start,
			EndRow:     end,
			RowCount:   end - start,
		})
	}
	return fragments
}

// ExternalFetchOptions groups per-collection external table parameters
// to keep function signatures clean.
type ExternalFetchOptions struct {
	CollectionID int64
	SpecExtfs    map[string]string // extfs overrides from ExternalSpec (already prefix-keyed)
}

// FetchFragmentsFromExternalSourceWithRange
// only processes files in the [fileIndexBegin, fileIndexEnd) range from the explore result.
// If exploreManifestPath is non-empty, reads file list from the manifest instead of re-exploring.
// This enables parallel refresh by splitting files across multiple tasks.
func FetchFragmentsFromExternalSourceWithRange(
	ctx context.Context,
	format string,
	columns []string,
	externalSource string,
	storageConfig *indexpb.StorageConfig,
	fileIndexBegin, fileIndexEnd int64,
	exploreManifestPath string,
	opts ExternalFetchOptions,
	fragmentRowLimit ...int64,
) ([]Fragment, error) {
	log := log.Ctx(ctx)

	rowLimit := int64(DefaultFragmentRowLimit)
	if len(fragmentRowLimit) > 0 && fragmentRowLimit[0] > 0 {
		rowLimit = fragmentRowLimit[0]
	}

	var fileInfos []FileInfo
	var err error
	extfsPrefix := ExtfsPrefixForCollection(opts.CollectionID)
	extfsOverrides := BuildExtfsOverrides(externalSource, storageConfig, extfsPrefix, opts.SpecExtfs)

	if exploreManifestPath == "" {
		return nil, fmt.Errorf("explore manifest path is required")
	}

	exploreStart := time.Now()
	fileInfos, err = ReadFileInfosFromManifestPath(exploreManifestPath, storageConfig)
	exploreDuration := time.Since(exploreStart)
	if err != nil {
		return nil, fmt.Errorf("failed to read explore manifest: %w", err)
	}
	log.Info("Read file list from explore manifest",
		zap.String("manifestPath", exploreManifestPath),
		zap.Int("totalFileCount", len(fileInfos)),
		zap.Duration("readDuration", exploreDuration))

	// Slice to assigned range
	if fileIndexEnd > int64(len(fileInfos)) {
		fileIndexEnd = int64(len(fileInfos))
	}
	if fileIndexBegin >= int64(len(fileInfos)) {
		return nil, fmt.Errorf("fileIndexBegin %d >= total files %d", fileIndexBegin, len(fileInfos))
	}
	fileInfos = fileInfos[fileIndexBegin:fileIndexEnd]

	if len(fileInfos) == 0 {
		return nil, fmt.Errorf("no files in range [%d, %d)", fileIndexBegin, fileIndexEnd)
	}

	// Fetch row counts concurrently (same logic as FetchFragmentsFromExternalSource)
	getFileInfoStart := time.Now()
	needInfo := make([]int, 0, len(fileInfos))
	for i, fi := range fileInfos {
		if fi.NumRows <= 0 {
			needInfo = append(needInfo, i)
		}
	}

	rowCounts := make([]int64, len(fileInfos))
	for i, fi := range fileInfos {
		rowCounts[i] = fi.NumRows
	}

	const getFileInfoWorkers = 16
	if len(needInfo) > 0 {
		var firstErr error
		var errOnce sync.Once
		tasks := make(chan int, len(needInfo))
		var wg sync.WaitGroup

		workers := getFileInfoWorkers
		if workers > len(needInfo) {
			workers = len(needInfo)
		}
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer wg.Done()
				for i := range tasks {
					fetchedInfo, err := GetFileInfo(format, fileInfos[i].FilePath, storageConfig, extfsOverrides)
					if err != nil {
						errOnce.Do(func() {
							firstErr = fmt.Errorf("failed to get file info for %s: %w", fileInfos[i].FilePath, err)
						})
						return
					}
					rowCounts[i] = fetchedInfo.NumRows
				}
			}()
		}
		for _, idx := range needInfo {
			if ctx.Err() != nil || firstErr != nil {
				break
			}
			tasks <- idx
		}
		close(tasks)
		wg.Wait()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if firstErr != nil {
			return nil, firstErr
		}
	}

	getFileInfoDuration := time.Since(getFileInfoStart)
	log.Info("GetFileInfo phase completed (with range)",
		zap.Int("filesNeedingInfo", len(needInfo)),
		zap.Int("totalFiles", len(fileInfos)),
		zap.Duration("getFileInfoDuration", getFileInfoDuration))

	var fragments []Fragment
	fragmentIDGenerator := NewFragmentIDGenerator(0)
	for i, fi := range fileInfos {
		fileFragments := SplitFileToFragments(fi.FilePath, rowCounts[i], rowLimit, fragmentIDGenerator)
		fragments = append(fragments, fileFragments...)
	}

	if len(fragments) == 0 {
		return nil, fmt.Errorf("no data files in range [%d, %d)", fileIndexBegin, fileIndexEnd)
	}

	log.Info("Created fragments from file range",
		zap.Int("totalFragments", len(fragments)),
		zap.Int("fileCount", len(fileInfos)),
		zap.Int64("fileIndexBegin", fileIndexBegin),
		zap.Int64("fileIndexEnd", fileIndexEnd))

	return fragments, nil
}

// FetchFragmentsFromFileInfos creates fragments from a pre-computed file list.
// This is the fastest path: DataCoord already explored and split files, so DN
// only needs to fetch row counts (if missing) and create fragments.
func FetchFragmentsFromFileInfos(
	ctx context.Context,
	format string,
	columns []string,
	externalSource string,
	storageConfig *indexpb.StorageConfig,
	fileInfos []FileInfo,
	opts ExternalFetchOptions,
	fragmentRowLimit ...int64,
) ([]Fragment, error) {
	log := log.Ctx(ctx)

	rowLimit := int64(DefaultFragmentRowLimit)
	if len(fragmentRowLimit) > 0 && fragmentRowLimit[0] > 0 {
		rowLimit = fragmentRowLimit[0]
	}

	extfsPrefix := ExtfsPrefixForCollection(opts.CollectionID)
	extfsOverrides := BuildExtfsOverrides(externalSource, storageConfig, extfsPrefix, opts.SpecExtfs)

	if len(fileInfos) == 0 {
		return nil, fmt.Errorf("no assigned files")
	}

	log.Info("FetchFragmentsFromFileInfos: using pre-assigned file list",
		zap.Int("numFiles", len(fileInfos)))

	// Fetch row counts for files that don't have them yet
	getFileInfoStart := time.Now()
	needInfo := make([]int, 0)
	for i, fi := range fileInfos {
		if fi.NumRows <= 0 {
			needInfo = append(needInfo, i)
		}
	}

	rowCounts := make([]int64, len(fileInfos))
	for i, fi := range fileInfos {
		rowCounts[i] = fi.NumRows
	}

	const getFileInfoWorkers = 16
	if len(needInfo) > 0 {
		var firstErr error
		var errOnce sync.Once
		tasks := make(chan int, len(needInfo))
		var wg sync.WaitGroup

		workers := getFileInfoWorkers
		if workers > len(needInfo) {
			workers = len(needInfo)
		}
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer wg.Done()
				for i := range tasks {
					fetchedInfo, err := GetFileInfo(format, fileInfos[i].FilePath, storageConfig, extfsOverrides)
					if err != nil {
						errOnce.Do(func() {
							firstErr = fmt.Errorf("failed to get file info for %s: %w", fileInfos[i].FilePath, err)
						})
						return
					}
					rowCounts[i] = fetchedInfo.NumRows
				}
			}()
		}
		for _, idx := range needInfo {
			if ctx.Err() != nil || firstErr != nil {
				break
			}
			tasks <- idx
		}
		close(tasks)
		wg.Wait()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if firstErr != nil {
			return nil, firstErr
		}
	}

	getFileInfoDuration := time.Since(getFileInfoStart)
	log.Info("GetFileInfo phase completed (assigned files)",
		zap.Int("filesNeedingInfo", len(needInfo)),
		zap.Int("totalFiles", len(fileInfos)),
		zap.Duration("getFileInfoDuration", getFileInfoDuration))

	var fragments []Fragment
	fragmentIDGenerator := NewFragmentIDGenerator(0)
	for i, fi := range fileInfos {
		fileFragments := SplitFileToFragments(fi.FilePath, rowCounts[i], rowLimit, fragmentIDGenerator)
		fragments = append(fragments, fileFragments...)
	}

	if len(fragments) == 0 {
		return nil, fmt.Errorf("no fragments created from %d assigned files", len(fileInfos))
	}

	log.Info("Created fragments from assigned files",
		zap.Int("totalFragments", len(fragments)),
		zap.Int("fileCount", len(fileInfos)))

	return fragments, nil
}

// BuildCurrentSegmentFragments builds segment to fragments mapping from current segments.
// It reads fragment info from manifest if available, otherwise creates virtual fragments.
// Returns error if a segment has a manifest path but the manifest cannot be read.
func BuildCurrentSegmentFragments(
	segments []*datapb.SegmentInfo,
	storageConfig *indexpb.StorageConfig,
) (SegmentFragments, error) {
	result := make(SegmentFragments)
	for _, seg := range segments {
		// Try to read from manifest if available
		if seg.GetManifestPath() != "" && storageConfig != nil {
			fragments, err := ReadFragmentsFromManifest(seg.GetManifestPath(), storageConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to read manifest for segment %d at %s: %w",
					seg.GetID(), seg.GetManifestPath(), err)
			}
			if len(fragments) > 0 {
				result[seg.GetID()] = fragments
				continue
			}
			log.Warn("manifest returned 0 fragments, using virtual fragment",
				zap.Int64("segmentID", seg.GetID()),
				zap.String("manifestPath", seg.GetManifestPath()))
		}

		// Virtual fragment for segments without manifest (initial state)
		result[seg.GetID()] = []Fragment{
			{
				FragmentID: seg.GetID(),
				FilePath:   "",
				StartRow:   0,
				EndRow:     seg.GetNumOfRows(),
				RowCount:   seg.GetNumOfRows(),
			},
		}
	}
	return result, nil
}

// CreateSegmentManifest creates a manifest file for a segment with the given fragments.
// This is a convenience wrapper around CreateManifestForSegment.
func CreateSegmentManifest(
	ctx context.Context,
	collectionID int64,
	segmentID int64,
	format string,
	columns []string,
	fragments []Fragment,
	storageConfig *indexpb.StorageConfig,
) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	// Build manifest base path
	basePath := fmt.Sprintf("external/%d/segments/%d", collectionID, segmentID)

	manifestPath, err := CreateManifestForSegment(
		basePath,
		columns,
		format,
		fragments,
		storageConfig,
	)
	if err != nil {
		return "", err
	}

	return manifestPath, nil
}

// CreateSegmentManifestWithBasePath creates a manifest file with a custom base path.
// This allows creating temporary manifests that will be renamed later.
func CreateSegmentManifestWithBasePath(
	ctx context.Context,
	basePath string,
	format string,
	columns []string,
	fragments []Fragment,
	storageConfig *indexpb.StorageConfig,
) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	manifestPath, err := CreateManifestForSegment(
		basePath,
		columns,
		format,
		fragments,
		storageConfig,
	)
	if err != nil {
		return "", err
	}

	return manifestPath, nil
}

// GetColumnNamesFromSchema extracts column names from schema.
// For external collections: only includes fields with ExternalField set
// (system fields like __virtual_pk__ are skipped as they don't exist in parquet data).
// For normal collections: uses field name for all fields.
func GetColumnNamesFromSchema(schema *schemapb.CollectionSchema) []string {
	if schema == nil {
		return nil
	}

	isExternal := schema.GetExternalSource() != ""
	var columns []string
	for _, field := range schema.GetFields() {
		extField := field.GetExternalField()
		if extField != "" {
			columns = append(columns, extField)
		} else if !isExternal {
			// Non-external collections: use field name
			columns = append(columns, field.GetName())
		}
		// External collections: skip fields without ExternalField
		// (e.g., __virtual_pk__, RowID, Timestamp)
	}
	return columns
}
