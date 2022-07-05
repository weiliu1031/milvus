package session

import (
	"context"
	"fmt"
	"sync"

	grpcquerynodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"go.uber.org/zap"
)

type ErrNodeNotFound struct {
	nodeID int64
}

func (err *ErrNodeNotFound) Error() string {
	return fmt.Sprintf("node %d not found", err.nodeID)
}

func newErrNodeNotFound(nodeID int64) *ErrNodeNotFound {
	return &ErrNodeNotFound{nodeID}
}

var IsErrNodeNotFound = func(err error) bool {
	_, ok := err.(*ErrNodeNotFound)
	return ok
}

// FIXME: remove useless clients
// Cluster is used to send requests to QueryNodes and manage connections
type Cluster struct {
	clients
	nodeManager *NodeManager
}

func (c *Cluster) LoadSegments(ctx context.Context, nodeID int64, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	var status *commonpb.Status
	var err error
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		status, err = cli.LoadSegments(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return status, err
}

func (c *Cluster) WatchDmChannels(ctx context.Context, nodeID int64, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	var status *commonpb.Status
	var err error
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		status, err = cli.WatchDmChannels(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return status, err
}

func (c *Cluster) WatchDeltaChannels(ctx context.Context, nodeID int64, req *querypb.WatchDeltaChannelsRequest) (*commonpb.Status, error) {
	var status *commonpb.Status
	var err error
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		status, err = cli.WatchDeltaChannels(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return status, err
}

func (c *Cluster) ReleaseCollection(ctx context.Context, nodeID int64, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	var status *commonpb.Status
	var err error
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		status, err = cli.ReleaseCollection(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return status, err
}

func (c *Cluster) ReleasePartitions(ctx context.Context, nodeID int64, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	var status *commonpb.Status
	var err error
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		status, err = cli.ReleasePartitions(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return status, err
}

func (c *Cluster) ReleaseSegments(ctx context.Context, nodeID int64, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	var status *commonpb.Status
	var err error
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		status, err = cli.ReleaseSegments(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return status, err
}

func (c *Cluster) SyncReplicaSegments(ctx context.Context, nodeID int64, req *querypb.SyncReplicaSegmentsRequest) (*commonpb.Status, error) {
	var status *commonpb.Status
	var err error
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		status, err = cli.SyncReplicaSegments(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return status, err
}

func (c *Cluster) send(ctx context.Context, nodeID int64, fn func(cli *grpcquerynodeclient.Client)) error {
	node := c.nodeManager.Get(nodeID)
	if node == nil {
		return newErrNodeNotFound(nodeID)
	}

	cli, err := c.clients.getOrCreate(ctx, node)
	if err != nil {
		return err
	}

	fn(cli)
	return nil
}

func (c *Cluster) Close() {
	c.clients.closeAll()
}

func NewCluster(nodeManager *NodeManager) Cluster {
	return Cluster{
		clients:     newClients(),
		nodeManager: nodeManager,
	}
}

type clients struct {
	sync.RWMutex
	clients map[int64]*grpcquerynodeclient.Client // nodeID -> client
}

func (c *clients) getOrCreate(ctx context.Context, node *NodeInfo) (*grpcquerynodeclient.Client, error) {
	if cli := c.get(node.ID()); cli != nil {
		return cli, nil
	}

	newCli, err := createNewClient(ctx, node.Addr())
	if err != nil {
		return nil, err
	}
	c.set(node.ID(), newCli)
	return c.get(node.ID()), nil
}

func createNewClient(ctx context.Context, addr string) (*grpcquerynodeclient.Client, error) {
	newCli, err := grpcquerynodeclient.NewClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err = newCli.Init(); err != nil {
		return nil, err
	}
	if err = newCli.Start(); err != nil {
		return nil, err
	}
	return newCli, nil
}

func (c *clients) set(nodeID int64, client *grpcquerynodeclient.Client) {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.clients[nodeID]; ok {
		if err := client.Stop(); err != nil {
			log.Warn("close new created client error", zap.Int64("nodeID", nodeID), zap.Error(err))
			return
		}
		log.Info("use old client", zap.Int64("nodeID", nodeID))
	}
	c.clients[nodeID] = client
}

func (c *clients) get(nodeID int64) *grpcquerynodeclient.Client {
	c.RLock()
	defer c.RUnlock()
	return c.clients[nodeID]
}

func (c *clients) close(nodeID int64) {
	c.Lock()
	defer c.Unlock()
	if cli, ok := c.clients[nodeID]; ok {
		if err := cli.Stop(); err != nil {
			log.Warn("error occured during stopping client", zap.Int64("nodeID", nodeID), zap.Error(err))
		}
		delete(c.clients, nodeID)
	}
}

func (c *clients) closeAll() {
	c.Lock()
	defer c.Unlock()
	for nodeID, cli := range c.clients {
		if err := cli.Stop(); err != nil {
			log.Warn("error occured during stopping client", zap.Int64("nodeID", nodeID), zap.Error(err))
		}
	}
}

func newClients() clients {
	return clients{clients: make(map[int64]*grpcquerynodeclient.Client)}
}
