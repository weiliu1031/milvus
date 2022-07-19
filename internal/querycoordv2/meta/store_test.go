package meta

import (
	"testing"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/stretchr/testify/suite"
)

type StoreTestSuite struct {
	suite.Suite
	store MetaStore
}

func (suite *StoreTestSuite) SetupTest() {
	kv := memkv.NewMemoryKV()
	suite.store = NewMetaStore(kv)
}

func (suite *StoreTestSuite) TearDownTest() {}

func (suite *StoreTestSuite) TestLoadRelease() {
	var err error
	// empty partitions error
	err = suite.store.Load(1, []int64{})
	suite.Equal(ErrEmptyPartitions, err)

	var ret map[int64][]int64
	ret, err = suite.store.GetPartitions()
	suite.NoError(err)
	suite.Empty(ret)
	err = suite.store.Load(1, []int64{1, 2})
	suite.NoError(err)
	ret, err = suite.store.GetPartitions()
	suite.NoError(err)
	suite.Equal(map[int64][]int64{1: {1, 2}}, ret)
	err = suite.store.Release(1, []int64{1})
	suite.NoError(err)
	ret, err = suite.store.GetPartitions()
	suite.NoError(err)
	suite.Equal(map[int64][]int64{1: {2}}, ret)
}

func TestStoreSuite(t *testing.T) {
	suite.Run(t, new(StoreTestSuite))
}
