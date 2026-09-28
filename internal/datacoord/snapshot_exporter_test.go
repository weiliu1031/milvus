// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"os"
	"testing"

	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type snapshotExporterCopierTarget struct{ storage.SizedCrossBucketCopier }

func clearSegmentNonInsertFiles(segment *datapb.SegmentDescription) {
	segment.Statslogs = nil
	segment.Deltalogs = nil
	segment.Bm25Statslogs = nil
	segment.IndexFiles = nil
	segment.TextIndexFiles = nil
	segment.JsonKeyIndexFiles = nil
}

func snapshotExportStorageConfig(bucket string) *indexpb.StorageConfig {
	cfg := &indexpb.StorageConfig{BucketName: bucket}
	if bucket != "" {
		cfg.Address = "s3.us-west-2.amazonaws.com"
		cfg.CloudProvider = objectstorage.CloudProviderAWS
		cfg.Region = "us-west-2"
		cfg.UseSSL = true
	}
	return cfg
}

func newSnapshotExporterCopierMock(
	t *testing.T,
	copyFn func(context.Context, string, string, string, string) error,
) storage.CrossBucketCopier {
	t.Helper()
	target := &snapshotExporterCopierTarget{}
	metadataCopy := mockey.Mock((*snapshotExporterCopierTarget).CopyCrossBucket).To(copyFn).Build()
	mockCopy := mockey.Mock((*snapshotExporterCopierTarget).CopyCrossBucketAndGetSize).To(func(ctx context.Context, srcBucket, src, dstBucket, dst string) (int64, error) {
		if err := copyFn(ctx, srcBucket, src, dstBucket, dst); err != nil {
			return 0, err
		}
		// Real-file fixtures report their copied bytes; synthetic copy fixtures
		// only exercise scheduling and use zero-byte results.
		info, err := os.Stat(dst)
		if os.IsNotExist(err) {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		return info.Size(), nil
	}).Build()
	t.Cleanup(func() { mockCopy.UnPatch(); metadataCopy.UnPatch() })
	return target
}
