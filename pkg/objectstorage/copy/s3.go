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

package copy

import (
	"context"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const minioSingleCopyObjectMaxSize = 5 * 1024 * 1024 * 1024

type S3Options struct {
	MultipartThreshold int64
	PartSize           int64
	PartConcurrency    int
	PartRetryAttempts  int
	// DisableMultipart supports XML-compatible services, such as GCS, whose
	// whole-object copy supports large objects but whose API has no part copy.
	DisableMultipart bool
}

func DefaultS3Options() S3Options {
	return S3Options{MultipartThreshold: 500 * 1024 * 1024,
		PartSize: minioCopyPartSize, PartConcurrency: minioCopyPartConcurrency,
		PartRetryAttempts: 10}
}

type s3Copier struct {
	client  *minio.Client
	options S3Options
}

// NewS3Copier retains the caller's client and never changes or closes it.
// SDK errors are returned intact; callers may apply their boundary mapping.
func NewS3Copier(client *minio.Client, options S3Options) (Copier, error) {
	if client == nil || options.MultipartThreshold < 0 || options.PartSize < minioCopyPartSize ||
		options.PartSize > minioSingleCopyObjectMaxSize || options.PartConcurrency <= 0 || options.PartRetryAttempts <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("invalid S3 copy configuration")
	}
	return &s3Copier{client: client, options: options}, nil
}

func (s *s3Copier) Copy(ctx context.Context, req Request) (Result, error) {
	srcBucket, srcObjectName := req.Source.Bucket, req.Source.Key
	dstBucket, dstObjectName := req.Destination.Bucket, req.Destination.Key
	threshold := s.options.MultipartThreshold
	srcInfo, err := s.client.StatObject(ctx, srcBucket, srcObjectName, minio.StatObjectOptions{})
	if err != nil {
		return Result{}, err
	}
	if srcInfo.Size < 0 {
		return Result{}, merr.WrapErrDataIntegrityMsg("source object size cannot be negative")
	}
	srcOpts := minio.CopySrcOptions{Bucket: srcBucket, Object: srcObjectName}
	dstOpts := minio.CopyDestOptions{Bucket: dstBucket, Object: dstObjectName}
	// GCS's XML API has no multipart copy: x-amz-copy-source-range (emitted by
	// UploadPartCopy) has no x-goog-* equivalent. Its whole-object copy has no
	// 5GiB cap though, so GCP always takes the single-copy path.
	multipart := srcInfo.Size >= max(threshold, minioCopyPartSize) || srcInfo.Size > minioSingleCopyObjectMaxSize
	if !multipart || s.options.DisableMultipart {
		// minio-go v7.0.73 Client.CopyObject returns the ETag from the HTTP
		// header, but S3 puts it in the XML body. Core.CopyObject reads the
		// body correctly, so use it without upgrading the shared SDK.
		core := minio.Core{Client: s.client}
		var result minio.ObjectInfo
		result, err = core.CopyObject(ctx, srcBucket, srcObjectName, dstBucket, dstObjectName, nil, srcOpts, minio.PutObjectOptions{})
		// Both SDK paths can decode HTTP 200 <Error> as an empty result.
		// Require the body ETag before reporting success or counting bytes;
		// an empty ETag does not confirm a copy and remains a retriable I/O error.
		if err == nil && strings.TrimSpace(result.ETag) == "" {
			return Result{}, merr.WrapErrIoFailedMsg("single-object copy returned an empty ETag")
		}
	} else {
		err = s.copyObjectMultipart(ctx, dstOpts, srcOpts, srcInfo)
	}
	if err != nil {
		return Result{}, err
	}
	return Result{Bytes: srcInfo.Size, BytesKnown: true}, nil
}
