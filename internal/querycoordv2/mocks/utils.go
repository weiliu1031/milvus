package mocks

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

// GenerateEtcdConfig returns a etcd config with a random root path,
// NOTE: for test only
func GenerateEtcdConfig() paramtable.EtcdConfig {
	config := params.Params.EtcdCfg
	rand.Seed(time.Now().UnixNano())
	suffix := "-test-query-Coord" + strconv.FormatInt(rand.Int63(), 10)
	config.MetaRootPath = config.MetaRootPath + suffix
	return config
}

func RandomIncrementIDAllocator() func() (int64, error) {
	id := rand.Int63() / 2
	return func() (int64, error) {
		return atomic.AddInt64(&id, 1), nil
	}
}

func ErrorIDAllocator() func() (int64, error) {
	return func() (int64, error) {
		return 0, fmt.Errorf("failed to allocate ID")
	}
}
