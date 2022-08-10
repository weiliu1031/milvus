package job

import (
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
)

func checkLoadCollectionParameter(collectionMgr *meta.CollectionManager, collection *meta.Collection) error {
	if collectionMgr.Exist(collection.GetCollectionID()) {
		old := collectionMgr.GetCollection(collection.GetCollectionID())
		if old == nil {
			return ErrLoadParameterMismatched
		}
		return ErrCollectionLoaded
	}
	return nil
}
