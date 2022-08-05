package dist

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"go.uber.org/zap"
)

const (
	distReqInterval = 500 * time.Millisecond
	distReqTimeout  = 3 * time.Second
	maxFailureTimes = 3
)

type distHandler struct {
	nodeID      int64
	client      *session.Cluster
	c           chan struct{}
	wg          sync.WaitGroup
	nodeManager *session.NodeManager
}

func (dh *distHandler) start(ctx context.Context) {
	defer dh.wg.Done()
	log.Info("start dist handelr", zap.Int64("nodeID", dh.nodeID))
	ticker := time.NewTicker(distReqInterval)
	failures := 0
	for {
		select {
		case <-ctx.Done():
			log.Info("close dist handler due to context done", zap.Int64("nodeID", dh.nodeID))
		case <-dh.c:
			log.Info("close dist handelr", zap.Int64("nodeID", dh.nodeID))
		case <-ticker.C:
			cctx, cancel := context.WithTimeout(ctx, distReqTimeout)
			resp, err := dh.client.GetDataDistribution(cctx, dh.nodeID, nil)
			cancel()

			if err != nil || resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				failures++
				dh.logFailureInfo(resp, err)
			} else {
				failures = 0
				dh.handleDistResp(resp)
			}

			if failures >= maxFailureTimes {
				log.RatedInfo(30.0, fmt.Sprintf("can not get data distribution from node %d for %d times", dh.nodeID, failures))
				// TODO: kill the querynode server and stop the loop?
			}
		}
	}
}

func (dh *distHandler) logFailureInfo(resp *querypb.GetDataDistributionResponse, err error) {
	if err != nil {
		log.Warn("failed to get data distribution",
			zap.Int64("nodeID", dh.nodeID),
			zap.Error(err))
	} else if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("failed to get data distribution",
			zap.Int64("nodeID", dh.nodeID),
			zap.Any("error code", resp.GetStatus().GetErrorCode()),
			zap.Any("reason", resp.GetStatus().GetReason()))
	}
}

func (dh *distHandler) handleDistResp(resp *querypb.GetDataDistributionResponse) {
	node := dh.nodeManager.Get(resp.GetNodeID())
	if node != nil {
		node.UpdateStats(
			session.WithSegmentCnt(len(resp.GetSegmentIDs())),
			session.WithChannelCnt(len(resp.GetChannels())),
		)
	}
}

func (dh *distHandler) close() {
	close(dh.c)
	dh.wg.Wait()
}

func newDistHandler(
	ctx context.Context,
	nodeID int64,
	client *session.Cluster,
) *distHandler {
	h := &distHandler{
		nodeID: nodeID,
		client: client,
		c:      make(chan struct{}),
	}
	h.wg.Add(1)
	go h.start(ctx)
	return h
}
