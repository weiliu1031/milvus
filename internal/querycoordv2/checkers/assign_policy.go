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

package checkers

import (
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
)

// AssignPolicy encapsulates the node selection logic for segment and channel assignment.
// It provides a unified interface for both SegmentChecker and ChannelChecker to
// determine which nodes are available for assignment based on replica configuration.
type AssignPolicy interface {
	// GetAssignableNodesForSegment returns the list of nodes that can be assigned to for segments.
	// It handles special cases like L0 segments that must be assigned to shard leaders.
	// Parameters:
	//   - replica: the replica to assign to
	//   - shard: the shard/channel name
	//   - isLevel0: whether the segments are L0 segments
	//   - leaderID: the leader node ID for the shard (used for L0 segments)
	// Returns the list of node IDs that can be used for assignment.
	GetAssignableNodesForSegment(replica *meta.Replica, shard string, isLevel0 bool, leaderID int64) []int64

	// GetAssignableNodesForChannel returns the list of nodes that can be assigned to for channels.
	// Parameters:
	//   - replica: the replica to assign to
	//   - channelName: the channel name
	// Returns the list of node IDs that can be used for assignment.
	GetAssignableNodesForChannel(replica *meta.Replica, channelName string) []int64
}

// DefaultAssignPolicy is the default implementation of AssignPolicy.
// It implements the standard node selection logic used by QueryCoord.
type DefaultAssignPolicy struct{}

// NewDefaultAssignPolicy creates a new DefaultAssignPolicy instance.
func NewDefaultAssignPolicy() *DefaultAssignPolicy {
	return &DefaultAssignPolicy{}
}

// GetAssignableNodesForSegment implements the node selection logic for segment assignment.
// It first tries to get shard-specific RW nodes, falling back to replica-wide RW nodes.
// For L0 segments, it returns only the shard leader node since L0 segments must be
// co-located with the channel on the same node.
func (p *DefaultAssignPolicy) GetAssignableNodesForSegment(replica *meta.Replica, shard string, isLevel0 bool, leaderID int64) []int64 {
	// L0 segment can only be assigned to shard leader's node
	if isLevel0 {
		return []int64{leaderID}
	}

	// First try to get shard-specific RW nodes
	rwNodes := replica.GetChannelRWNodes(shard)
	if len(rwNodes) == 0 {
		// Fall back to replica-wide RW nodes
		rwNodes = replica.GetRWNodes()
	}
	return rwNodes
}

// GetAssignableNodesForChannel implements the node selection logic for channel assignment.
// It first tries to get channel-specific RW nodes, falling back to replica-wide RW nodes.
func (p *DefaultAssignPolicy) GetAssignableNodesForChannel(replica *meta.Replica, channelName string) []int64 {
	// First try to get channel-specific RW nodes
	rwNodes := replica.GetChannelRWNodes(channelName)
	if len(rwNodes) == 0 {
		// Fall back to replica-wide RW nodes
		rwNodes = replica.GetRWNodes()
	}
	return rwNodes
}
