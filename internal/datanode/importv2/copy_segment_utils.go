// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importv2

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

// Copy Mode Implementation for Snapshot/Backup Import
//
// This file implements high-performance segment import by copying files directly
// instead of reading, parsing, and rewriting data. This is specifically designed
// for snapshot restore and backup import scenarios where data format is identical.
//
// IMPLEMENTATION APPROACH (v2.0):
// 1. Pre-calculate all file path mappings (source -> target) in one pass
// 2. Copy files sequentially using ChunkManager.Copy()
// 3. Preserve all binlog metadata (EntriesNum, Timestamps, LogSize) from source
// 4. Generate complete segment metadata with accurate row counts
//
// CURRENT LIMITATIONS:
// 1. Index metadata is not preserved (buildIndexInfoFromResults returns nil)
// 2. Sequential copying - may be slower for large file counts
// 3. First error stops entire operation (fail-fast behavior)
//
// WHY THIS APPROACH:
// - Direct file copying is 10-100x faster than data parsing/rewriting
// - Snapshot/backup scenarios guarantee data format compatibility
// - Metadata is preserved from source segment info (row counts, timestamps, etc.)
// - Simplified error handling - any copy failure aborts the entire operation
//
// FUTURE IMPROVEMENTS:
// - Implement parallel file copying with goroutine pool for better performance
// - Build proper index metadata from copied index files
// - Add optional retry logic for transient copy failures
// - Support partial success with detailed failure reporting
// - Add file integrity verification (checksums, size validation)
//
// SAFETY:
// - All file operations are validated and logged
// - Copy failures are properly detected and reported with full context
// - Fail-fast behavior prevents partial/inconsistent imports
//
// # CopySegmentAndIndexFiles copies all segment files and index files sequentially
//
// This function is the main entry point for copying segment data from source to target paths.
// It handles all types of segment files (insert, stats, delta, BM25 binlogs) and index files
// (vector/scalar, text, JSON key indexes).
//
// Process flow:
// 1. Validate input - ensure source has insert binlogs
// 2. Generate all file path mappings (source -> target) by replacing collection/partition/segment IDs
// 3. Execute all file copy operations sequentially via ChunkManager
// 4. Build segment metadata preserving source binlog information (row counts, timestamps, etc.)
// 5. Return segment info and index info (currently nil - to be implemented)
//
// Parameters:
//   - ctx: Context for cancellation and logging
//   - cm: ChunkManager for file operations (S3, MinIO, local storage, etc.)
//   - source: Source segment information containing all file paths and metadata
//   - target: Target collection/partition/segment IDs for path transformation
//   - logFields: Additional zap fields for contextual logging
//
// Returns:
//   - result: Complete CopySegmentResult with segment binlogs and index metadata
//   - error: First encountered copy error, or nil if all operations succeed
func CopySegmentAndIndexFiles(
	ctx context.Context,
	cm storage.ChunkManager,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	logFields []zap.Field,
) (*datapb.CopySegmentResult, error) {
	log.Info("start copying segment and index files")

	// Step 1: Collect all copy tasks (both segment binlogs and index files)
	mappings, err := createFileMappings(source, target)
	if err != nil {
		return nil, fmt.Errorf("failed to collect copy tasks: %w", err)
	}

	// Step 2: Execute all copy operations
	for src, dst := range mappings {
		log.Info("execute copy file",
			zap.String("sourcePath", src),
			zap.String("targetPath", dst))
		if err := cm.Copy(ctx, src, dst); err != nil {
			log.Warn("failed to copy file", append(logFields,
				zap.String("sourcePath", src),
				zap.String("targetPath", dst),
				zap.Error(err))...)
			return nil, fmt.Errorf("failed to copy file from %s to %s: %w", src, dst, err)
		}
	}

	log.Info("all files copied successfully", append(logFields,
		zap.Int("fileCount", len(mappings)))...)

	// Step 3: Build index metadata from source
	indexInfos, textIndexInfos, jsonKeyIndexInfos := buildIndexInfoFromSource(source, target, mappings)

	// Step 4: Generate segment metadata with path mappings
	segmentInfo, err := generateSegmentInfoFromSource(source, target, mappings)
	if err != nil {
		return nil, fmt.Errorf("failed to generate segment info: %v", err)
	}

	// Step 5: Build complete result combining segment info and index metadata
	result := &datapb.CopySegmentResult{
		SegmentId:         segmentInfo.GetSegmentID(),
		ImportedRows:      segmentInfo.GetImportedRows(),
		Binlogs:           segmentInfo.GetBinlogs(),
		Statslogs:         segmentInfo.GetStatslogs(),
		Deltalogs:         segmentInfo.GetDeltalogs(),
		Bm25Logs:          segmentInfo.GetBm25Logs(),
		IndexInfos:        indexInfos,
		TextIndexInfos:    textIndexInfos,
		JsonKeyIndexInfos: jsonKeyIndexInfos,
	}

	log.Info("copy segment and index files completed successfully",
		append(logFields,
			zap.Int64("importedRows", result.ImportedRows),
			zap.Int("binlogCount", len(result.Binlogs)),
			zap.Int("statslogCount", len(result.Statslogs)),
			zap.Int("deltalogCount", len(result.Deltalogs)),
			zap.Int("bm25logCount", len(result.Bm25Logs)),
			zap.Int("indexInfoCount", len(result.IndexInfos)),
			zap.Int("textIndexInfoCount", len(result.TextIndexInfos)),
			zap.Int("jsonKeyIndexInfoCount", len(result.JsonKeyIndexInfos)))...)

	return result, nil
}

// transformFieldBinlogs transforms source FieldBinlog list to destination by replacing paths
// using the pre-calculated mappings, while preserving all other metadata.
//
// This function is used to build the segment metadata that DataCoord needs for tracking
// the imported segment. All source binlog metadata is preserved except for the file paths,
// which are replaced using the mappings generated during the copy operation.
//
// Parameters:
//   - srcFieldBinlogs: Source field binlogs with original paths
//   - mappings: Pre-calculated map of source path -> target path
//   - countRows: If true, accumulate total row count from EntriesNum (for insert logs only)
//
// Returns:
//   - []*datapb.FieldBinlog: Transformed binlog list with target paths
//   - int64: Total row count (sum of EntriesNum from all binlogs if countRows=true, 0 otherwise)
//   - error: Always returns nil in current implementation
func transformFieldBinlogs(
	srcFieldBinlogs []*datapb.FieldBinlog,
	mappings map[string]string,
	countRows bool, // true for insert logs to count total rows
) ([]*datapb.FieldBinlog, int64, error) {
	result := make([]*datapb.FieldBinlog, 0, len(srcFieldBinlogs))
	var totalRows int64

	for _, srcFieldBinlog := range srcFieldBinlogs {
		dstFieldBinlog := &datapb.FieldBinlog{
			FieldID: srcFieldBinlog.GetFieldID(),
			Binlogs: make([]*datapb.Binlog, 0, len(srcFieldBinlog.GetBinlogs())),
		}

		for _, srcBinlog := range srcFieldBinlog.GetBinlogs() {
			if srcPath := srcBinlog.GetLogPath(); srcPath != "" {
				dstBinlog := &datapb.Binlog{
					EntriesNum:    srcBinlog.GetEntriesNum(),
					TimestampFrom: srcBinlog.GetTimestampFrom(),
					TimestampTo:   srcBinlog.GetTimestampTo(),
					LogPath:       mappings[srcPath],
					LogSize:       srcBinlog.GetLogSize(),
				}
				dstFieldBinlog.Binlogs = append(dstFieldBinlog.Binlogs, dstBinlog)

				if countRows {
					totalRows += srcBinlog.GetEntriesNum()
				}
			}
		}

		if len(dstFieldBinlog.Binlogs) > 0 {
			result = append(result, dstFieldBinlog)
		}
	}

	return result, totalRows, nil
}

// generateSegmentInfoFromSource generates ImportSegmentInfo from CopySegmentSource
// by transforming all binlog paths and preserving metadata.
//
// This function constructs the complete segment metadata that DataCoord uses to track
// the imported segment. It processes all four types of binlogs:
//   - Insert binlogs (required): Contains row data, row count is summed for ImportedRows
//   - Stats binlogs (optional): Contains statistics like min/max values
//   - Delta binlogs (optional): Contains delete operations
//   - BM25 binlogs (optional): Contains BM25 index data
//
// All source binlog metadata (EntriesNum, TimestampFrom, TimestampTo, LogSize) is preserved
// to maintain data integrity and enable proper query/compaction operations.
//
// Parameters:
//   - source: Source segment with original binlog paths and metadata
//   - target: Target IDs (collection/partition/segment) for segment identification
//   - mappings: Pre-calculated path mappings (source -> target)
//
// Returns:
//   - *datapb.ImportSegmentInfo: Complete segment metadata with target paths and row counts
//   - error: Error if any binlog transformation fails
func generateSegmentInfoFromSource(
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	mappings map[string]string,
) (*datapb.ImportSegmentInfo, error) {
	segmentInfo := &datapb.ImportSegmentInfo{
		SegmentID:    target.GetSegmentId(),
		ImportedRows: 0,
		Binlogs:      []*datapb.FieldBinlog{},
		Statslogs:    []*datapb.FieldBinlog{},
		Deltalogs:    []*datapb.FieldBinlog{},
		Bm25Logs:     []*datapb.FieldBinlog{},
	}

	// Process insert binlogs (count rows)
	binlogs, totalRows, err := transformFieldBinlogs(source.GetInsertBinlogs(), mappings, true)
	if err != nil {
		return nil, fmt.Errorf("failed to transform insert binlogs: %w", err)
	}
	segmentInfo.Binlogs = binlogs
	segmentInfo.ImportedRows = totalRows

	// Process stats binlogs (no row counting)
	statslogs, _, err := transformFieldBinlogs(source.GetStatsBinlogs(), mappings, false)
	if err != nil {
		return nil, fmt.Errorf("failed to transform stats binlogs: %w", err)
	}
	segmentInfo.Statslogs = statslogs

	// Process delta binlogs (no row counting)
	deltalogs, _, err := transformFieldBinlogs(source.GetDeltaBinlogs(), mappings, false)
	if err != nil {
		return nil, fmt.Errorf("failed to transform delta binlogs: %w", err)
	}
	segmentInfo.Deltalogs = deltalogs

	// Process BM25 binlogs (no row counting)
	bm25logs, _, err := transformFieldBinlogs(source.GetBm25Binlogs(), mappings, false)
	if err != nil {
		return nil, fmt.Errorf("failed to transform BM25 binlogs: %w", err)
	}
	segmentInfo.Bm25Logs = bm25logs

	return segmentInfo, nil
}

// generateTargetPath converts source file path to target path by replacing collection/partition/segment IDs
// Binlog path format: {bucket}/{log_type}/{collectionID}/{partitionID}/{segmentID}/{fieldID}/{logID}
// Example: files/insert_log/111/222/333/444/555.log -> files/insert_log/aaa/bbb/ccc/444/555.log
func generateTargetPath(sourcePath string, source *datapb.CopySegmentSource, target *datapb.CopySegmentTarget) (string, error) {
	// Convert IDs to strings for replacement
	targetCollectionIDStr := strconv.FormatInt(target.GetCollectionId(), 10)
	targetPartitionIDStr := strconv.FormatInt(target.GetPartitionId(), 10)
	targetSegmentIDStr := strconv.FormatInt(target.GetSegmentId(), 10)

	// Split path into parts
	parts := strings.Split(sourcePath, "/")

	// Find the log type index (insert_log, delta_log, stats_log, bm25_stats)
	// Path structure: .../log_type/collectionID/partitionID/segmentID/...
	logTypeIndex := -1
	for i, part := range parts {
		if part == "insert_log" || part == "delta_log" || part == "stats_log" || part == "bm25_stats" {
			logTypeIndex = i
			break
		}
	}

	if logTypeIndex == -1 || logTypeIndex+3 >= len(parts) {
		return "", fmt.Errorf("invalid binlog path structure: %s (expected log_type at a valid position)", sourcePath)
	}

	// Replace IDs in order: collectionID, partitionID, segmentID
	// log_type is at index logTypeIndex
	// collectionID is at index logTypeIndex + 1
	// partitionID is at index logTypeIndex + 2
	// segmentID is at index logTypeIndex + 3
	parts[logTypeIndex+1] = targetCollectionIDStr
	parts[logTypeIndex+2] = targetPartitionIDStr
	parts[logTypeIndex+3] = targetSegmentIDStr

	return strings.Join(parts, "/"), nil
}

// createFileMappings generates path mappings for all segment files in a single pass.
//
// This function iterates through all file types (binlogs and indexes) and generates
// target paths by replacing collection/partition/segment IDs. The resulting mappings
// are used for both file copying and metadata generation.
//
// Supported file types:
//   - Binlog types: Insert (required), Delta, Stats, BM25
//   - Index types: Vector/Scalar indexes, Text indexes, JSON Key indexes
//
// Path transformation:
//   - Binlogs: {bucket}/log_type/coll/part/seg/... -> {bucket}/log_type/NEW_coll/NEW_part/NEW_seg/...
//   - Indexes: Similar ID replacement based on index type path structure
//
// Parameters:
//   - source: Source segment with original file paths
//   - target: Target IDs (collection/partition/segment) for path transformation
//
// Returns:
//   - map[string]string: Source path -> target path for all files
//   - error: Error if path generation fails for any file
func createFileMappings(
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
) (map[string]string, error) {
	mappings := make(map[string]string)

	fileTypeList := []string{BinlogTypeInsert, BinlogTypeDelta, BinlogTypeStats, BinlogTypeBM25, IndexTypeVectorScalar, IndexTypeText, IndexTypeJSONKey}
	for _, fileType := range fileTypeList {
		switch fileType {
		case BinlogTypeInsert:
			for _, fieldBinlog := range source.GetInsertBinlogs() {
				for _, binlog := range fieldBinlog.GetBinlogs() {
					if sourcePath := binlog.GetLogPath(); sourcePath != "" {
						targetPath, err := generateTargetPath(sourcePath, source, target)
						if err != nil {
							return nil, fmt.Errorf("failed to generate target path for %s file %s: %w", fileType, sourcePath, err)
						}
						mappings[sourcePath] = targetPath
					}
				}
			}
		case BinlogTypeDelta:
			for _, fieldBinlog := range source.GetDeltaBinlogs() {
				for _, binlog := range fieldBinlog.GetBinlogs() {
					if sourcePath := binlog.GetLogPath(); sourcePath != "" {
						targetPath, err := generateTargetPath(sourcePath, source, target)
						if err != nil {
							return nil, fmt.Errorf("failed to generate target path for %s file %s: %w", fileType, sourcePath, err)
						}
						mappings[sourcePath] = targetPath
					}
				}
			}
		case BinlogTypeStats:
			for _, fieldBinlog := range source.GetStatsBinlogs() {
				for _, binlog := range fieldBinlog.GetBinlogs() {
					if sourcePath := binlog.GetLogPath(); sourcePath != "" {
						targetPath, err := generateTargetPath(sourcePath, source, target)
						if err != nil {
							return nil, fmt.Errorf("failed to generate target path for %s file %s: %w", fileType, sourcePath, err)
						}
						mappings[sourcePath] = targetPath
					}
				}
			}
		case BinlogTypeBM25:
			for _, fieldBinlog := range source.GetBm25Binlogs() {
				for _, binlog := range fieldBinlog.GetBinlogs() {
					if sourcePath := binlog.GetLogPath(); sourcePath != "" {
						targetPath, err := generateTargetPath(sourcePath, source, target)
						if err != nil {
							return nil, fmt.Errorf("failed to generate target path for %s file %s: %w", fileType, sourcePath, err)
						}
						mappings[sourcePath] = targetPath
					}
				}
			}
		case IndexTypeVectorScalar:
			for _, indexInfo := range source.GetIndexFiles() {
				for _, sourcePath := range indexInfo.GetIndexFilePaths() {
					targetPath, err := generateTargetIndexPath(sourcePath, source, target, fileType)
					if err != nil {
						return nil, fmt.Errorf("failed to generate target %s index path for %s: %w", fileType, sourcePath, err)
					}
					mappings[sourcePath] = targetPath
				}
			}

		case IndexTypeText:
			for _, indexInfo := range source.GetTextIndexFiles() {
				for _, sourcePath := range indexInfo.GetFiles() {
					targetPath, err := generateTargetIndexPath(sourcePath, source, target, fileType)
					if err != nil {
						return nil, fmt.Errorf("failed to generate target %s index path for %s: %w", fileType, sourcePath, err)
					}
					mappings[sourcePath] = targetPath
				}
			}

		case IndexTypeJSONKey:
			for _, indexInfo := range source.GetJsonKeyIndexFiles() {
				for _, sourcePath := range indexInfo.GetFiles() {
					targetPath, err := generateTargetIndexPath(sourcePath, source, target, fileType)
					if err != nil {
						return nil, fmt.Errorf("failed to generate target %s index path for %s: %w", fileType, sourcePath, err)
					}
					mappings[sourcePath] = targetPath
				}
			}

		default:
			return nil, fmt.Errorf("unsupported index type: %s", fileType)
		}
	}

	return mappings, nil
}

// buildIndexInfoFromSource builds complete index metadata from source information.
//
// This function extracts and transforms all index metadata (vector/scalar, text, JSON)
// from the source segment, converting file paths to target paths using the provided mappings.
//
// Parameters:
//   - source: Source segment with index file information
//   - target: Target IDs for the segment
//   - mappings: Pre-calculated source->target path mappings
//
// Returns:
//   - Vector/Scalar index metadata (fieldID -> VectorScalarIndexInfo)
//   - Text index metadata (fieldID -> TextIndexStats)
//   - JSON Key index metadata (fieldID -> JsonKeyStats)
func buildIndexInfoFromSource(
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	mappings map[string]string,
) (
	map[int64]*datapb.VectorScalarIndexInfo,
	map[int64]*datapb.TextIndexStats,
	map[int64]*datapb.JsonKeyStats,
) {
	// Process vector/scalar indexes
	indexInfos := make(map[int64]*datapb.VectorScalarIndexInfo)
	for _, srcIndex := range source.GetIndexFiles() {
		// Transform index file paths using mappings
		targetPaths := make([]string, 0, len(srcIndex.GetIndexFilePaths()))
		for _, srcPath := range srcIndex.GetIndexFilePaths() {
			if targetPath, ok := mappings[srcPath]; ok {
				targetPaths = append(targetPaths, targetPath)
			}
		}

		indexInfos[srcIndex.GetFieldID()] = &datapb.VectorScalarIndexInfo{
			FieldId:             srcIndex.GetFieldID(),
			IndexId:             srcIndex.GetIndexID(),
			BuildId:             srcIndex.GetBuildID(),
			Version:             srcIndex.GetIndexVersion(),
			IndexFilePaths:      targetPaths,
			IndexSize:           int64(srcIndex.GetSerializedSize()),
			CurrentIndexVersion: srcIndex.GetCurrentIndexVersion(),
			// Note: CurrentScalarIndexVersion is not available in IndexFilePathInfo
			// It will be 0 (default value)
		}
	}

	// Process text indexes - transform file paths
	textIndexInfos := make(map[int64]*datapb.TextIndexStats)
	for fieldID, srcText := range source.GetTextIndexFiles() {
		// Transform text index file paths using mappings
		targetFiles := make([]string, 0, len(srcText.GetFiles()))
		for _, srcFile := range srcText.GetFiles() {
			if targetFile, ok := mappings[srcFile]; ok {
				targetFiles = append(targetFiles, targetFile)
			}
		}

		textIndexInfos[fieldID] = &datapb.TextIndexStats{
			FieldID:    srcText.GetFieldID(),
			Version:    srcText.GetVersion(),
			BuildID:    srcText.GetBuildID(),
			Files:      targetFiles,
			LogSize:    srcText.GetLogSize(),
			MemorySize: srcText.GetMemorySize(),
		}
	}

	// Process JSON Key indexes - transform file paths
	jsonKeyIndexInfos := make(map[int64]*datapb.JsonKeyStats)
	for fieldID, srcJson := range source.GetJsonKeyIndexFiles() {
		// Transform JSON index file paths using mappings
		targetFiles := make([]string, 0, len(srcJson.GetFiles()))
		for _, srcFile := range srcJson.GetFiles() {
			if targetFile, ok := mappings[srcFile]; ok {
				targetFiles = append(targetFiles, targetFile)
			}
		}

		jsonKeyIndexInfos[fieldID] = &datapb.JsonKeyStats{
			FieldID:                srcJson.GetFieldID(),
			Version:                srcJson.GetVersion(),
			BuildID:                srcJson.GetBuildID(),
			Files:                  targetFiles,
			JsonKeyStatsDataFormat: srcJson.GetJsonKeyStatsDataFormat(),
			MemorySize:             srcJson.GetMemorySize(),
		}
	}

	return indexInfos, textIndexInfos, jsonKeyIndexInfos
}

// ============================================================================
// File Type Constants
// ============================================================================

// File type constants used for path identification and generation.
// These constants match the directory names in Milvus storage paths.
const (
	BinlogTypeInsert      = "insert_log"
	BinlogTypeStats       = "stats_log"
	BinlogTypeDelta       = "delta_log"
	BinlogTypeBM25        = "bm25_log"
	IndexTypeVectorScalar = "index_files"
	IndexTypeText         = "text_log"
	IndexTypeJSONKey      = "json_key_index_log"
)

// generateTargetIndexPath is the unified function for generating target paths for all index types
// The indexType parameter specifies which type of index path to generate
//
// Supported index types (use constants):
//   - IndexTypeVectorScalar: Vector/Scalar Index path format
//     {bucket}/index_files/{collection_id}/{partition_id}/{segment_id}/{field_id}/{index_id}/{build_id}/file
//   - IndexTypeText: Text Index path format
//     {rootPath}/text_log/{build_id}/{version}/{collection_id}/{partition_id}/{segment_id}/{field_id}/file
//   - IndexTypeJSONKey: JSON Key Index path format
//     {rootPath}/json_key_index_log/{build_id}/{version}/{collection_id}/{partition_id}/{segment_id}/{field_id}/file
//
// Examples:
// generateTargetIndexPath(..., IndexTypeVectorScalar):
//
//	files/index_files/111/222/333/444/555/666/scalar_index -> files/index_files/aaa/bbb/ccc/444/555/666/scalar_index
//
// generateTargetIndexPath(..., IndexTypeText):
//
//	files/text_log/123/1/111/222/333/444/index_file -> files/text_log/123/1/aaa/bbb/ccc/444/index_file
//
// generateTargetIndexPath(..., IndexTypeJSONKey):
//
//	files/json_key_index_log/123/1/111/222/333/444/index_file -> files/json_key_index_log/123/1/aaa/bbb/ccc/444/index_file
func generateTargetIndexPath(
	sourcePath string,
	source *datapb.CopySegmentSource,
	target *datapb.CopySegmentTarget,
	indexType string,
) (string, error) {
	// Split path into parts
	parts := strings.Split(sourcePath, "/")

	// Determine keyword and offsets based on index type
	var keywordIdx int
	var collectionOffset, partitionOffset, segmentOffset int

	// Find the keyword position in the path
	keywordIdx = -1
	for i, part := range parts {
		if part == indexType {
			keywordIdx = i
			break
		}
	}

	if keywordIdx == -1 {
		return "", fmt.Errorf("keyword '%s' not found in path: %s", indexType, sourcePath)
	}

	// Set offsets based on index type
	switch indexType {
	case IndexTypeVectorScalar:
		// Vector/Scalar index: index_files/coll/part/seg/field/index/build
		collectionOffset = 1
		partitionOffset = 2
		segmentOffset = 3
	case IndexTypeText, IndexTypeJSONKey:
		// Text/JSON index: text_log|json_key_index_log/build/ver/coll/part/seg/field
		collectionOffset = 3
		partitionOffset = 4
		segmentOffset = 5
	default:
		return "", fmt.Errorf("unsupported index type: %s (expected '%s', '%s', or '%s')",
			indexType, IndexTypeVectorScalar, IndexTypeText, IndexTypeJSONKey)
	}

	// Validate path structure has enough components
	if keywordIdx+segmentOffset >= len(parts) {
		return "", fmt.Errorf("invalid %s path structure: %s (expected '%s' with at least %d components after it)",
			indexType, sourcePath, indexType, segmentOffset+1)
	}

	// Replace IDs at specified offsets
	parts[keywordIdx+collectionOffset] = strconv.FormatInt(target.GetCollectionId(), 10)
	parts[keywordIdx+partitionOffset] = strconv.FormatInt(target.GetPartitionId(), 10)
	parts[keywordIdx+segmentOffset] = strconv.FormatInt(target.GetSegmentId(), 10)

	return path.Join(parts...), nil
}
