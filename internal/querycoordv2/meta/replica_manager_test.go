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

package meta

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/stretchr/testify/suite"
)

type ReplicaManagerSuite struct {
	suite.Suite

	nodes          []int64
	collections    []int64
	replicaNumbers []int32
	idAllocator    func() (int64, error)
	kv             kv.MetaKv
	store          Store
	mgr            *ReplicaManager
	meta           *Meta
}

func (suite *ReplicaManagerSuite) SetupSuite() {
	Params.Init()

	suite.nodes = []int64{1, 2, 3}
	suite.collections = []int64{100, 101, 102}
	suite.replicaNumbers = []int32{1, 2, 3}
}

func (suite *ReplicaManagerSuite) SetupTest() {
	var err error
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())
	suite.store = NewMetaStore(suite.kv)

	suite.meta = NewMeta(suite.idAllocator, suite.store, session.NewNodeManager())
	suite.idAllocator = RandomIncrementIDAllocator()
	suite.mgr = NewReplicaManager(suite.idAllocator, suite.store)
	suite.spawnAndPutAll()
}

func (suite *ReplicaManagerSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *ReplicaManagerSuite) TestSpawn() {
	mgr := suite.mgr

	for i, collection := range suite.collections {
		replicas, err := mgr.SpawnReplicas(collection, map[string]int32{DefaultResourceGroupName: suite.replicaNumbers[i]})
		suite.NoError(err)
		suite.Len(replicas, int(suite.replicaNumbers[i]))
	}

	mgr.idAllocator = ErrorIDAllocator()
	for i, collection := range suite.collections {
		_, err := mgr.SpawnReplicas(collection, map[string]int32{DefaultResourceGroupName: suite.replicaNumbers[i]})
		suite.Error(err)
	}
}

func (suite *ReplicaManagerSuite) TestGet() {
	mgr := suite.mgr

	for _, collection := range suite.collections {
		replicas := mgr.GetByCollection(collection)
		for _, replica := range replicas {
			suite.Equal(collection, replica.GetCollectionID())
			suite.Equal(replica, mgr.Get(replica.GetID()))
			nodes, err := suite.meta.ResourceManager.GetNodes(replica.GetResourceGroup())
			suite.NoError(err)
			suite.Len(nodes, 3)
		}
	}
}

func (suite *ReplicaManagerSuite) TestRecover() {
	mgr := suite.mgr

	// Clear data in memory, and then recover from meta store
	suite.clearMemory()
	mgr.Recover(suite.collections)
	suite.TestGet()

	// Test recover from 2.1 meta store
	replicaInfo := milvuspb.ReplicaInfo{
		ReplicaID:    2100,
		CollectionID: 1000,
		NodeIds:      []int64{1, 2, 3},
	}
	value, err := proto.Marshal(&replicaInfo)
	suite.NoError(err)
	suite.kv.Save(ReplicaMetaPrefixV1+"/2100", string(value))

	suite.clearMemory()
	mgr.Recover(append(suite.collections, 1000))
	replica := mgr.Get(2100)
	suite.NotNil(replica)
	suite.EqualValues(1000, replica.CollectionID)
}

func (suite *ReplicaManagerSuite) TestRemove() {
	mgr := suite.mgr

	for _, collection := range suite.collections {
		err := mgr.RemoveCollection(collection)
		suite.NoError(err)

		replicas := mgr.GetByCollection(collection)
		suite.Empty(replicas)
	}

	// Check whether the replicas are also removed from meta store
	mgr.Recover(suite.collections)
	for _, collection := range suite.collections {
		replicas := mgr.GetByCollection(collection)
		suite.Empty(replicas)
	}
}

// func (suite *ReplicaManagerSuite) TestNodeManipulate() {
// 	mgr := suite.mgr

// 	firstNode := suite.nodes[0]
// 	newNode := suite.nodes[len(suite.nodes)-1] + 1
// 	// Add a new node for the replica with node 1 of all collections,
// 	// then remove the node 1
// 	for _, collection := range suite.collections {
// 		replica := mgr.GetByCollectionAndNode(collection, firstNode)
// 		err := mgr.AddNode(replica.GetID(), newNode)
// 		suite.NoError(err)

// 		replica = mgr.GetByCollectionAndNode(collection, newNode)
// 		suite.Contains(replica.Nodes, newNode)
// 		suite.Contains(replica.Replica.GetNodes(), newNode)

// 		err = mgr.RemoveNode(replica.GetID(), firstNode)
// 		suite.NoError(err)
// 		replica = mgr.GetByCollectionAndNode(collection, firstNode)
// 		suite.Nil(replica)
// 	}

// 	// Check these modifications are applied to meta store
// 	suite.clearMemory()
// 	mgr.Recover(suite.collections)
// 	for _, collection := range suite.collections {
// 		replica := mgr.GetByCollectionAndNode(collection, firstNode)
// 		suite.Nil(replica)

// 		replica = mgr.GetByCollectionAndNode(collection, newNode)
// 		suite.Contains(replica.Nodes, newNode)
// 		suite.Contains(replica.Replica.GetNodes(), newNode)
// 	}
// }

func (suite *ReplicaManagerSuite) spawnAndPutAll() {
	mgr := suite.mgr

	for i, collection := range suite.collections {
		replicas, err := mgr.SpawnReplicas(collection, map[string]int32{DefaultResourceGroupName: suite.replicaNumbers[i]})
		suite.NoError(err)
		suite.Len(replicas, int(suite.replicaNumbers[i]))
		err = mgr.Put(replicas...)
		suite.NoError(err)
	}

	for _, node := range suite.nodes {
		suite.meta.ResourceManager.AssignNode(DefaultResourceGroupName, node)
	}
}

func (suite *ReplicaManagerSuite) clearMemory() {
	suite.mgr.replicas = make(map[int64]*Replica)
}

func TestReplicaManager(t *testing.T) {
	suite.Run(t, new(ReplicaManagerSuite))
}
