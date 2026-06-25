package compaction

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type EntityFilter interface {
	Filtered(pk any, ts typeutil.Timestamp, expirationTimeMicros int64) bool

	GetExpiredCount() int
	GetDeletedCount() int
	GetDeltalogDeleteCount() int
	GetMissingDeleteCount() int
}

type RestoreTsRange struct {
	LowerBound typeutil.Timestamp
	UpperBound typeutil.Timestamp
}

func NewRestoreTsRanges(ranges []*datapb.RestoreTsRange) []RestoreTsRange {
	if len(ranges) == 0 {
		return nil
	}
	out := make([]RestoreTsRange, 0, len(ranges))
	for _, r := range ranges {
		if r == nil || r.GetLowerBound() >= r.GetUpperBound() {
			continue
		}
		out = append(out, RestoreTsRange{
			LowerBound: r.GetLowerBound(),
			UpperBound: r.GetUpperBound(),
		})
	}
	return out
}

func IsInRestoreTsRanges(ts typeutil.Timestamp, ranges []RestoreTsRange) bool {
	for _, r := range ranges {
		if r.LowerBound < ts && ts < r.UpperBound {
			return true
		}
	}
	return false
}

func NewEntityFilter(
	deletedPkTs map[interface{}]typeutil.Timestamp,
	ttl int64,
	currTime time.Time,
	commitTs typeutil.Timestamp,
	restoreTsRanges []RestoreTsRange,
) EntityFilter {
	return newEntityFilter(deletedPkTs, ttl, currTime, commitTs, restoreTsRanges)
}

type EntityFilterImpl struct {
	deletedPkTs map[interface{}]typeutil.Timestamp // pk2ts
	ttl         int64                              // nanoseconds
	currentTime time.Time
	// commitTs is SegmentInfo.commit_timestamp for import/CDC segments.
	// When non-zero, row timestamps in binlogs are stale (they predate the
	// actual write time). isEntityExpired and isEntityDeleted both use
	// max(row_ts, commitTs) so that no row is prematurely expired and no
	// pre-commit delete is applied.
	commitTs typeutil.Timestamp

	restoreTsRanges []RestoreTsRange

	expiredCount int
	deletedCount int
}

func newEntityFilter(
	deletedPkTs map[interface{}]typeutil.Timestamp,
	ttl int64,
	currTime time.Time,
	commitTs typeutil.Timestamp,
	restoreTsRanges []RestoreTsRange,
) *EntityFilterImpl {
	if deletedPkTs == nil {
		deletedPkTs = make(map[interface{}]typeutil.Timestamp)
	}
	return &EntityFilterImpl{
		deletedPkTs:     deletedPkTs,
		ttl:             ttl,
		currentTime:     currTime,
		commitTs:        commitTs,
		restoreTsRanges: restoreTsRanges,
	}
}

func (filter *EntityFilterImpl) Filtered(pk any, ts typeutil.Timestamp, expirationTimeMicros int64) bool {
	if filter.isEntityInRestoreRange(ts) {
		return true
	}

	if filter.isEntityDeleted(pk, ts) {
		filter.deletedCount++
		return true
	}

	// Filtering expired entity
	if filter.isEntityExpired(ts) {
		filter.expiredCount++
		return true
	}

	if filter.isEntityExpiredByTTLField(expirationTimeMicros) {
		filter.expiredCount++
		return true
	}
	return false
}

func (filter *EntityFilterImpl) GetExpiredCount() int {
	return filter.expiredCount
}

func (filter *EntityFilterImpl) GetDeletedCount() int {
	return filter.deletedCount
}

func (filter *EntityFilterImpl) GetDeltalogDeleteCount() int {
	return len(filter.deletedPkTs)
}

func (filter *EntityFilterImpl) GetMissingDeleteCount() int {
	diff := filter.GetDeltalogDeleteCount() - filter.GetDeletedCount()
	if diff <= 0 {
		diff = 0
	}
	return diff
}

func (filter *EntityFilterImpl) isEntityInRestoreRange(entityTs typeutil.Timestamp) bool {
	effectiveTs := tsoutil.EffectiveTimestamp(entityTs, filter.commitTs)
	return IsInRestoreTsRanges(effectiveTs, filter.restoreTsRanges)
}

func (filter *EntityFilterImpl) isEntityDeleted(pk interface{}, pkTs typeutil.Timestamp) bool {
	if deleteTs, ok := filter.deletedPkTs[pk]; ok {
		// For import/CDC segments the binlog row_ts predates the actual commit time.
		// A delete with del_ts < commit_ts must NOT take effect (the row did not exist
		// at that time), so compare against the same effective ts that visibility and
		// expiry use. Strict < is preserved so upserts (insert_ts == delete_ts) still
		// keep the inserted row.
		effectiveTs := tsoutil.EffectiveTimestamp(pkTs, filter.commitTs)
		if effectiveTs < deleteTs {
			return true
		}
	}
	return false
}

func (filter *EntityFilterImpl) isEntityExpired(entityTs typeutil.Timestamp) bool {
	// entity expire is not enabled if duration <= 0
	if filter.ttl <= 0 {
		return false
	}

	// For import/CDC segments, row timestamps in binlogs may predate the actual
	// commit time.  Use whichever is larger so a row is never marked expired
	// due to an outdated timestamp alone.
	entityTime, _ := tsoutil.ParseTS(tsoutil.EffectiveTimestamp(entityTs, filter.commitTs))

	// this dur can represents 292 million years before or after 1970, enough for milvus
	// ttl calculation
	dur := filter.currentTime.UnixMilli() - entityTime.UnixMilli()

	// filter.ttl is nanoseconds
	return filter.ttl/int64(time.Millisecond) <= dur
}

func (filter *EntityFilterImpl) isEntityExpiredByTTLField(expirationTimeMicros int64) bool {
	// entity expire is not enabled if expirationTimeMicros < 0
	if expirationTimeMicros < 0 {
		return false
	}

	// entityExpireTs is microseconds
	return filter.currentTime.UnixMicro() >= expirationTimeMicros
}
