package session

import (
	"context"
	"fmt"
	"sync"
	"time"

	grpcquerynodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"go.uber.org/zap"
)

const updateTickerDuration = 1 * time.Minute

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

// Cluster is used to send requests to QueryNodes and manage connections
type Cluster struct {
	*clients
	nodeManager *NodeManager
	wg          sync.WaitGroup
	ch          chan struct{}
}

func (c *Cluster) updateLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(updateTickerDuration)
	for {
		select {
		case <-c.ch:
			log.Info("cluster closed")
			return
		case <-ticker.C:
			nodes := c.clients.getAllNodeIDs()
			for _, id := range nodes {
				if c.nodeManager.Get(id) == nil {
					c.clients.close(id)
				}
			}
		}
	}
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

func (c *Cluster) UnsubDmChannel(ctx context.Context, nodeID int64, req *querypb.UnsubDmChannelRequest) (*commonpb.Status, error) {
	var status *commonpb.Status
	var err error
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		status, err = cli.UnsubDmChannel(ctx, req)
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

func (c *Cluster) GetDataDistribution(ctx context.Context, nodeID int64, req *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error) {
	var resp *querypb.GetDataDistributionResponse
	var err error
	err1 := c.send(ctx, nodeID, func(cli *grpcquerynodeclient.Client) {
		resp, err = cli.GetDataDistribution(ctx, req)
	})
	if err1 != nil {
		return nil, err1
	}
	return resp, err
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
	close(c.ch)
	c.wg.Wait()
}

func NewCluster(nodeManager *NodeManager) *Cluster {
	c := &Cluster{
		clients:     newClients(),
		nodeManager: nodeManager,
		ch:          make(chan struct{}),
	}
	c.wg.Add(1)
	go c.updateLoop()
	return c
}

type clients struct {
	sync.RWMutex
	clients map[int64]*grpcquerynodeclient.Client // nodeID -> client
}

func (c *clients) getAllNodeIDs() []int64 {
	c.RLock()
	defer c.RUnlock()

	ret := make([]int64, 0, len(c.clients))
	for k := range c.clients {
		ret = append(ret, k)
	}
	return ret
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

func newClients() *clients {
	return &clients{clients: make(map[int64]*grpcquerynodeclient.Client)}
}
