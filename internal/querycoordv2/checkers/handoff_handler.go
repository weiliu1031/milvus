package checkers

import (
	"context"
	"sync"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util"
)

type HandoffHandler struct {
	client            *etcdkv.EtcdKV
	c                 chan struct{}
	wg                *sync.WaitGroup
	target            *meta.TargetManager
	collectionManager *meta.CollectionManager

	revision int64
}

func (hh *HandoffHandler) Start(ctx context.Context) {
	defer hh.wg.Done()
	log.Info("Start watch segment handoff loop")

	watchChan := hh.client.WatchWithRevision(util.HandoffSegmentPrefix, hh.revision+1)
	for {
		select {
		case <-ctx.Done():
			log.Info("Close handoff handler due to context done!")
		case <-hh.c:
			log.Info("close handoff handler")

		case resp, ok := <-watchChan:
			if !ok {
				log.Error("Watch segment handoff loop failed because watch channel is closed!")
				panic("failed to handle handoff event, handoff handler exit...")
			}

			if err := resp.Err(); err != nil {
				log.Error("receive error handoff event from etcd, %s, %s", zap.String("prefix", util.HandoffSegmentPrefix),
					zap.Error(err))
				panic("failed to handle handoff event, handoff handler exit...")
			}

			for _, event := range resp.Events {
				segmentInfo := &querypb.SegmentInfo{}
				err := proto.Unmarshal(event.Kv.Value, segmentInfo)
				if err != nil {
					log.Error("watch segment handoff loop failed", zap.Error(err))
					continue
				}

				switch event.Type {
				case mvccpb.PUT:
					hh.handleHandOffEvent(segmentInfo)
				default:
					log.Warn("receive handoff event", zap.String("type", event.Type.String()))
				}
			}
		}
	}
}

func (hh *HandoffHandler) handleHandOffEvent(segment *querypb.SegmentInfo) {
	if hh.isCollectionLoaded(segment) {
		segmentInfo := &datapb.SegmentInfo{
			ID:                  segment.SegmentID,
			CollectionID:        segment.CollectionID,
			PartitionID:         segment.PartitionID,
			InsertChannel:       segment.DmChannel,
			CreatedByCompaction: segment.GetCreatedByCompaction(),
			CompactionFrom:      segment.GetCompactionFrom(),
		}

		hh.target.HandoffSegment(segmentInfo, segmentInfo.CompactionFrom...)
	} else {
		log.Debug("Handoff event trigger failed due to collection/partition is not loaded!",
			zap.Int64("segmentID", segment.SegmentID),
			zap.Int64("CollectionID", segment.CollectionID),
			zap.Int64("PartitionID", segment.PartitionID))
	}
}

func (hh *HandoffHandler) isCollectionLoaded(segmentInfo *querypb.SegmentInfo) bool {
	return hh.collectionManager.Exist(segmentInfo.CollectionID) && hh.collectionManager.Exist(segmentInfo.PartitionID)
}

func (hh *HandoffHandler) Close() {
	close(hh.c)
	hh.wg.Wait()
}
