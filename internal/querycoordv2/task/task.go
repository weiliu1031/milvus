package task

import (
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type Task interface {
	SetOnDone(func())
	SetOnFail(func())
}

type LoadSegmentTask struct {
	SegmentID UniqueID
	NodeID    UniqueID
}

type ReleaseSegmentTask struct {
	SegmentID UniqueID
	NodeID UniqueID
}
