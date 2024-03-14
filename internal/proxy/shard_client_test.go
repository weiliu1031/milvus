package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
)

func genShardLeaderInfo(channel string, leaderIDs []UniqueID) map[string][]nodeInfo {
	leaders := make(map[string][]nodeInfo)
	nodeInfos := make([]nodeInfo, len(leaderIDs))
	for i, id := range leaderIDs {
		nodeInfos[i] = nodeInfo{
			nodeID:  id,
			address: "fake",
		}
	}
	leaders[channel] = nodeInfos
	return leaders
}

func TestShardClientMgr_UpdateShardLeaders_CreatorNil(t *testing.T) {
	mgr := newShardClientMgr(withShardClientCreator(nil))
	mgr.clientCreator = nil
	leaders := genShardLeaderInfo("c1", []UniqueID{1, 2, 3})
	err := mgr.UpdateShardLeaders(nil, leaders)
	assert.Error(t, err)
}

func TestShardClientMgr_UpdateShardLeaders_Empty(t *testing.T) {
	mockCreator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
		return &mocks.MockQueryNodeClient{}, nil
	}
	mgr := newShardClientMgr(withShardClientCreator(mockCreator))

	_, err := mgr.GetClient(context.Background(), UniqueID(1))
	assert.Error(t, err)

	err = mgr.UpdateShardLeaders(nil, nil)
	assert.NoError(t, err)
	_, err = mgr.GetClient(context.Background(), UniqueID(1))
	assert.Error(t, err)

	leaders := genShardLeaderInfo("c1", []UniqueID{1, 2, 3})
	err = mgr.UpdateShardLeaders(leaders, nil)
	assert.NoError(t, err)
}

func TestShardClientMgr_UpdateShardLeaders_NonEmpty(t *testing.T) {
	mgr := newShardClientMgr()
	leaders := genShardLeaderInfo("c1", []UniqueID{1, 2, 3})
	err := mgr.UpdateShardLeaders(nil, leaders)
	assert.NoError(t, err)

	_, err = mgr.GetClient(context.Background(), UniqueID(1))
	assert.NoError(t, err)

	newLeaders := genShardLeaderInfo("c1", []UniqueID{2, 3})
	err = mgr.UpdateShardLeaders(leaders, newLeaders)
	assert.NoError(t, err)

	_, err = mgr.GetClient(context.Background(), UniqueID(1))
	assert.Error(t, err)
}

func TestShardClientMgr_UpdateShardLeaders_Ref(t *testing.T) {
	mgr := newShardClientMgr()
	leaders := genShardLeaderInfo("c1", []UniqueID{1, 2, 3})

	for i := 0; i < 2; i++ {
		err := mgr.UpdateShardLeaders(nil, leaders)
		assert.NoError(t, err)
	}

	partLeaders := genShardLeaderInfo("c1", []UniqueID{1})

	_, err := mgr.GetClient(context.Background(), UniqueID(1))
	assert.NoError(t, err)

	err = mgr.UpdateShardLeaders(partLeaders, nil)
	assert.NoError(t, err)

	_, err = mgr.GetClient(context.Background(), UniqueID(1))
	assert.NoError(t, err)

	err = mgr.UpdateShardLeaders(partLeaders, nil)
	assert.NoError(t, err)

	_, err = mgr.GetClient(context.Background(), UniqueID(1))
	assert.Error(t, err)

	_, err = mgr.GetClient(context.Background(), UniqueID(2))
	assert.NoError(t, err)

	_, err = mgr.GetClient(context.Background(), UniqueID(3))
	assert.NoError(t, err)
}

func TestShardClientMgr_ShardLeadersPartialUpdate(t *testing.T) {
	mgr := newShardClientMgr()

	genNodeInfos := func(leaderIDs []UniqueID) []nodeInfo {
		nodeInfos := make([]nodeInfo, len(leaderIDs))
		for i, id := range leaderIDs {
			nodeInfos[i] = nodeInfo{
				nodeID:  id,
				address: "fake",
			}
		}
		return nodeInfos
	}

	oldLeaders := map[string][]nodeInfo{
		"ch1": genNodeInfos([]UniqueID{1, 2}),
	}

	newLeaders := map[string][]nodeInfo{
		"ch1": genNodeInfos([]UniqueID{2, 3}),
	}

	err := mgr.UpdateShardLeaders(oldLeaders, newLeaders)
	assert.NoError(t, err)

	_, err = mgr.GetClient(context.Background(), UniqueID(1))
	assert.Error(t, err)

	_, err = mgr.GetClient(context.Background(), UniqueID(2))
	assert.NoError(t, err)

	_, err = mgr.GetClient(context.Background(), UniqueID(3))
	assert.NoError(t, err)
}
