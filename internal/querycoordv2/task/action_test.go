package task

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

type ActionTestSuite struct {
	suite.Suite
}

func (suite *ActionTestSuite) TestSegmentAction() {
	action := NewSegmentAction()
}
