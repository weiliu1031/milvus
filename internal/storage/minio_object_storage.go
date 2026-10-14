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

package storage

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var _ ObjectStorage = (*MinioObjectStorage)(nil)

// minio-go caps a single CopyObject request at 5 GiB, regardless of the configured threshold.
const minioSingleCopyObjectMaxSize = 5 * 1024 * 1024 * 1024

type MinioObjectStorage struct {
	*minio.Client

	cloudProvider string
}

type ObjectReader struct {
	*minio.Object
}

func (or *ObjectReader) Size() (int64, error) {
	stat, err := or.Stat()
	if err != nil {
		return -1, err
	}
	return stat.Size, nil
}

func newMinioObjectStorageWithConfig(ctx context.Context, c *objectstorage.Config) (*MinioObjectStorage, error) {
	resolvedConfig := *c
	resolvedConfig.CloudProvider = objectstorage.ResolveCloudProvider(c)
	minIOClient, err := objectstorage.NewMinioClient(ctx, &resolvedConfig)
	if err != nil {
		return nil, err
	}
	return &MinioObjectStorage{Client: minIOClient, cloudProvider: resolvedConfig.CloudProvider}, nil
}

func (minioObjectStorage *MinioObjectStorage) GetObject(ctx context.Context, bucketName, objectName string, offset int64, size int64) (FileReader, error) {
	opts := minio.GetObjectOptions{}
	if offset > 0 {
		end := int64(0)
		if size > 0 {
			end = offset + size - 1
		}
		err := opts.SetRange(offset, end)
		if err != nil {
			mlog.Warn(ctx, "failed to set range", mlog.String("bucket", bucketName), mlog.String("path", objectName), mlog.Err(err))
			return nil, mapObjectStorageError(objectName, err)
		}
	}
	object, err := minioObjectStorage.Client.GetObject(ctx, bucketName, objectName, opts)
	if err != nil {
		return nil, mapObjectStorageError(objectName, err)
	}
	return &ObjectReader{
		Object: object,
	}, nil
}

func (minioObjectStorage *MinioObjectStorage) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64) error {
	_, err := minioObjectStorage.Client.PutObject(ctx, bucketName, objectName, reader, objectSize, minioObjectStorage.putObjectOptions())
	return mapObjectStorageError(objectName, err)
}

func (minioObjectStorage *MinioObjectStorage) putObjectOptions() minio.PutObjectOptions {
	if !paramtable.Get().MinioCfg.DisableAWSChunkedEncoding.GetAsBool() {
		return minio.PutObjectOptions{}
	}
	return minio.PutObjectOptions{
		DisableContentSha256: true,
	}
}

func (minioObjectStorage *MinioObjectStorage) StatObject(ctx context.Context, bucketName, objectName string) (int64, error) {
	info, err := minioObjectStorage.Client.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
	return info.Size, mapObjectStorageError(objectName, err)
}

func (minioObjectStorage *MinioObjectStorage) WalkWithObjects(ctx context.Context, bucketName string, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) (err error) {
	// if minio has lots of objects under the provided path
	// recursive = true may timeout during the recursive browsing the objects.
	// See also: https://github.com/milvus-io/milvus/issues/19095
	// So we can change the `ListObjectsMaxKeys` to limit the max keys by batch to avoid timeout.
	in := minioObjectStorage.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: recursive,
		MaxKeys:   paramtable.Get().MinioCfg.ListObjectsMaxKeys.GetAsInt(),
	})

	for object := range in {
		if object.Err != nil {
			return object.Err
		}
		if !walkFunc(&ChunkObjectInfo{FilePath: object.Key, ModifyTime: object.LastModified}) {
			return nil
		}
	}
	return nil
}

func (minioObjectStorage *MinioObjectStorage) RemoveObject(ctx context.Context, bucketName, objectName string) error {
	err := minioObjectStorage.Client.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	return mapObjectStorageError(objectName, err)
}

func (minioObjectStorage *MinioObjectStorage) CopyObjectCrossBucket(ctx context.Context, srcBucket, srcObjectName, dstBucket, dstObjectName string) error {
	multipartThreshold := paramtable.Get().MinioCfg.MultipartCopyThreshold.GetAsInt64()
	srcOpts := minio.CopySrcOptions{
		Bucket: srcBucket,
		Object: srcObjectName,
	}
	dstOpts := minio.CopyDestOptions{
		Bucket: dstBucket,
		Object: dstObjectName,
	}
	var srcInfo minio.ObjectInfo
	err := retryObjectCopy(ctx, srcObjectName, func() error {
		var err error
		srcInfo, err = minioObjectStorage.Client.StatObject(ctx, srcBucket, srcObjectName, minio.StatObjectOptions{})
		return err
	})
	if err != nil {
		return err
	}
	// Small objects use single copy even when a custom threshold is lower
	// than the multipart minimum. GCS XML does not support ranged multipart copy.
	if srcInfo.Size <= 10*1024*1024 || (srcInfo.Size < multipartThreshold && srcInfo.Size <= minioSingleCopyObjectMaxSize) || minioObjectStorage.cloudProvider == objectstorage.CloudProviderGCP {
		return retryObjectCopy(ctx, srcObjectName, func() error {
			info, err := minioObjectStorage.CopyObject(ctx, dstOpts, srcOpts)
			if err == nil && info.ETag == "" {
				return merr.WrapErrServiceUnavailableMsg("object copy returned an empty ETag")
			}
			return err
		})
	}
	return minioObjectStorage.copyMultipart(ctx, srcBucket, srcObjectName, dstBucket, dstObjectName, srcInfo.Size)
}

// retryObjectCopy follows milvus-backup's retry policy: retry every operation
// error, double the delay from 200 ms up to 3 s, and accumulate failed attempts.
func retryObjectCopy(ctx context.Context, object string, fn func() error) error {
	return retryObjectCopyAttempts(ctx, object, fn, 10)
}

func retryObjectCopyAttempts(ctx context.Context, object string, fn func() error, attempts int) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var accumulated error
	delay := 200 * time.Millisecond
	for i := 0; i < attempts; i++ {
		if err := fn(); err != nil {
			accumulated = errors.Join(accumulated, mapObjectStorageError(object, err))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return errors.Join(accumulated, ctx.Err())
			}
			delay = min(delay*2, 3*time.Second)
		} else {
			return nil
		}
	}
	return accumulated
}

// copyMultipart retries each range independently, keeping completed parts in the same upload.
func (s *MinioObjectStorage) copyMultipart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, size int64) (err error) {
	const maxParts int64 = 10000
	const maxPartSize int64 = 5 * 1024 * 1024 * 1024
	partSize := max(int64(10*1024*1024), (size-1)/maxParts+1)
	if size <= 10*1024*1024 || size > 5*1024*1024*1024*1024 || partSize > maxPartSize {
		return merr.WrapErrParameterInvalidMsg("object %s exceeds multipart copy size limits", srcObject)
	}
	core := minio.Core{Client: s.Client}
	// Initiation is not idempotent: do not replay it and leak an unknown upload ID.
	uploadID, err := core.NewMultipartUpload(ctx, dstBucket, dstObject, minio.PutObjectOptions{})
	if err != nil {
		return mapObjectStorageError(dstObject, err)
	}
	defer func() {
		if err == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if cleanupErr := retryObjectCopyAttempts(cleanupCtx, dstObject, func() error {
			return core.AbortMultipartUpload(cleanupCtx, dstBucket, dstObject, uploadID)
		}, 3); cleanupErr != nil {
			mlog.Warn(cleanupCtx, "failed to abort multipart copy", mlog.String("object", dstObject), mlog.Err(cleanupErr))
		}
	}()
	parts := make([]minio.CompletePart, (size-1)/partSize+1)
	group, copyCtx := errgroup.WithContext(ctx)
	group.SetLimit(10)
	for i := range parts {
		group.Go(func() error {
			return retryObjectCopy(copyCtx, srcObject, func() error {
				part, err := core.CopyObjectPart(copyCtx, srcBucket, srcObject, dstBucket, dstObject, uploadID, i+1, int64(i)*partSize, min(partSize, size-int64(i)*partSize), nil)
				if err != nil {
					return err
				}
				if part.ETag == "" {
					return merr.WrapErrServiceUnavailableMsg("part copy returned an empty ETag")
				}
				parts[i] = part
				return nil
			})
		})
	}
	if err = group.Wait(); err != nil {
		return err
	}
	// Completion may have succeeded despite a lost response. Do not blindly replay
	// an upload ID that may already have been consumed by the provider.
	_, err = core.CompleteMultipartUpload(ctx, dstBucket, dstObject, uploadID, parts, minio.PutObjectOptions{})
	return mapObjectStorageError(dstObject, err)
}
