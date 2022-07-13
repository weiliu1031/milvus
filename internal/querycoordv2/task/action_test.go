package task

import (
    "testing"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/suite"
)

type ActionTestSuite struct {
	suite.Suite
}

func (suite *ActionTestSuite) TestSegmentAction() {
	action := NewSegmentAction()
}