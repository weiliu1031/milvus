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
	// TODO(sunby): add ut
}

func TestStoreSuite(t *testing.T) {
	suite.Run(t, new(StoreTestSuite))
}
