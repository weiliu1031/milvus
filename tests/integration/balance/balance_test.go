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

package balance

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type BalanceTestSuit struct {
	integration.MiniClusterSuite
}

func (s *BalanceTestSuit) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "1000")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")

	s.Require().NoError(s.SetupEmbedEtcd())
}

func (s *BalanceTestSuit) initCollection(collectionName string, replica int, channelNum int, segmentNum int, segmentRowNum int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		dim    = 128
		dbName = ""
	)

	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := s.Cluster.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      int32(channelNum),
	})
	s.NoError(err)
	s.True(merr.Ok(createCollectionStatus))

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := s.Cluster.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.Status))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	for i := 0; i < segmentNum; i++ {
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, segmentRowNum, dim)
		hashKeys := integration.GenerateHashKeys(segmentRowNum)
		insertResult, err := s.Cluster.Proxy.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(segmentRowNum),
		})
		s.NoError(err)
		s.True(merr.Ok(insertResult.Status))

		// flush
		flushResp, err := s.Cluster.Proxy.Flush(ctx, &milvuspb.FlushRequest{
			DbName:          dbName,
			CollectionNames: []string{collectionName},
		})
		s.NoError(err)
		segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
		ids := segmentIDs.GetData()
		s.Require().NotEmpty(segmentIDs)
		s.Require().True(has)
		flushTs, has := flushResp.GetCollFlushTs()[collectionName]
		s.True(has)
		s.WaitForFlush(ctx, ids, flushTs, dbName, collectionName)
	}

	// create index
	createIndexStatus, err := s.Cluster.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(err)
	s.True(merr.Ok(createIndexStatus))
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	for i := 1; i < replica; i++ {
		s.Cluster.AddQueryNode()
	}

	// load
	loadStatus, err := s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  int32(replica),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.True(merr.Ok(loadStatus))
	s.WaitForLoad(ctx, collectionName)
	log.Info("initCollection Done")
}

func (s *BalanceTestSuit) TestBalanceOnSingleReplica() {
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 2, 2, 2000)

	ctx := context.Background()
	// disable compact
	s.Cluster.DataCoord.GcControl(ctx, &datapb.GcControlRequest{
		Base:    commonpbutil.NewMsgBase(),
		Command: datapb.GcCommand_Pause,
		Params: []*commonpb.KeyValuePair{
			{Key: "duration", Value: "3600"},
		},
	})
	defer s.Cluster.DataCoord.GcControl(ctx, &datapb.GcControlRequest{
		Base:    commonpbutil.NewMsgBase(),
		Command: datapb.GcCommand_Resume,
	})

	// add a querynode, expected balance happens
	qn := s.Cluster.AddQueryNode()

	// check segment number on new querynode
	s.Eventually(func() bool {
		resp, err := qn.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		return len(resp.Channels) == 1 && len(resp.Segments) == 2
	}, 30*time.Second, 1*time.Second)

	// check total segment number
	s.Eventually(func() bool {
		count := 0
		for _, node := range s.Cluster.GetAllQueryNodes() {
			resp1, err := node.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
			s.NoError(err)
			s.True(merr.Ok(resp1.GetStatus()))
			count += len(resp1.Segments)
		}
		return count == 4
	}, 10*time.Second, 1*time.Second)
}

func (s *BalanceTestSuit) TestBalanceOnMultiReplica() {
	ctx := context.Background()

	// disable compact
	s.Cluster.DataCoord.GcControl(ctx, &datapb.GcControlRequest{
		Base:    commonpbutil.NewMsgBase(),
		Command: datapb.GcCommand_Pause,
		Params: []*commonpb.KeyValuePair{
			{Key: "duration", Value: "3600"},
		},
	})
	defer s.Cluster.DataCoord.GcControl(ctx, &datapb.GcControlRequest{
		Base:    commonpbutil.NewMsgBase(),
		Command: datapb.GcCommand_Resume,
	})

	// init collection with 2 channel, each channel has 2 segment, each segment has 2000 row
	// and load it with 2 replicas on 2 nodes.
	// then we add 2 query node, after balance happens, expected each node have 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 2, 2, 2, 2000)

	resp, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{CollectionName: name})
	s.NoError(err)
	s.Len(resp.Replicas, 2)

	// add a querynode, expected balance happens
	qn1 := s.Cluster.AddQueryNode()
	qn2 := s.Cluster.AddQueryNode()

	// check segment num on new query node
	s.Eventually(func() bool {
		resp, err := qn1.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		return len(resp.Channels) == 1 && len(resp.Segments) == 2
	}, 30*time.Second, 1*time.Second)

	s.Eventually(func() bool {
		resp, err := qn2.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		return len(resp.Channels) == 1 && len(resp.Segments) == 2
	}, 30*time.Second, 1*time.Second)

	// check total segment num
	s.Eventually(func() bool {
		count := 0
		for _, node := range s.Cluster.GetAllQueryNodes() {
			resp1, err := node.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
			s.NoError(err)
			s.True(merr.Ok(resp1.GetStatus()))
			count += len(resp1.Segments)
		}
		return count == 8
	}, 10*time.Second, 1*time.Second)
}

func TestBalance(t *testing.T) {
	suite.Run(t, new(BalanceTestSuit))
}
