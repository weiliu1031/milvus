package session

type stats struct {
	memStats
	segmentCnt int64
	channelCnt int64
}

func newStats() stats {
	return stats{
    memStats: newMemStats(),
	}
}

type memStats struct {
	capacity     int64
	available    int64
	preAllocated int64
}

func (s *memStats) setAvailable(available int64) {
	s.available = available
}

func (s *memStats) getAvailable() int64 {
	return s.available
}

func (s *memStats) setCapacity(cap int64) {
	s.capacity = cap
}
func (s *memStats) getCapacity() int64 {
	return s.capacity
}

func (s *memStats) getRemaining() int64 {
	return s.available - s.preAllocated
}

func (s *memStats) preAllocate(space int64) bool {
	if s.available >= space+s.preAllocated {
		s.preAllocated += space
		return true
	}
	return false
}

func (s *memStats) release(space int64) {
	s.preAllocated -= space
}

func newMemStats() memStats {
	return memStats{
		capacity:     0,
		available:    0,
		preAllocated: 0,
	}
}
