package task

import (
    "testing"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/suite"
)

type SchedulerTestSuite struct {
	suite.Suite
	scheduler *Scheduler
}

func (suite *SchedulerTestSuite) SetupSuite() {
	suite.scheduler = NewScheduler()
}