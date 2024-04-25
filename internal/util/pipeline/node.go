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

package pipeline

import (
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type Node interface {
	Name() string
	MaxQueueLength() int32
	Operate(in Msg) Msg
	Start()
	Close()
}

type nodeCtx struct {
	node         Node
	inputChannel chan Msg
	next         *nodeCtx
	checker      *timerecord.GroupChecker
}

func newNodeCtx(node Node) *nodeCtx {
	return &nodeCtx{
		node:         node,
		inputChannel: make(chan Msg, node.MaxQueueLength()),
	}
}

type BaseNode struct {
	name           string
	maxQueueLength int32
}

// Return name of Node
func (node *BaseNode) Name() string {
	return node.name
}

// length of pipeline input chnnel
func (node *BaseNode) MaxQueueLength() int32 {
	return node.maxQueueLength
}

// Start implementing Node, base node does nothing when starts
func (node *BaseNode) Start() {}

// Close implementing Node, base node does nothing when stops
func (node *BaseNode) Close() {}

func NewBaseNode(name string, maxQueryLength int32) *BaseNode {
	return &BaseNode{
		name:           name,
		maxQueueLength: maxQueryLength,
	}
}
