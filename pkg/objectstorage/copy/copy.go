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

// Package copy provides provider-side object copies using caller-owned clients.
package copy

import "context"

type Object struct {
	Bucket string
	Key    string
}

type Request struct {
	Source      Object
	Destination Object
}

type Result struct {
	Bytes      int64
	BytesKnown bool
}

type Copier interface {
	Copy(context.Context, Request) (Result, error)
}

// Func adapts an existing provider copy implementation without taking ownership
// of its client. It is also useful for callers that already resolve credentials.
type Func func(context.Context, Request) (Result, error)

func (f Func) Copy(ctx context.Context, req Request) (Result, error) {
	return f(ctx, req)
}
