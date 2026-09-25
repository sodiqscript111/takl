package metrics

import (
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Counter struct {
	value atomic.Uint64
}

func (c *Counter) Add(v uint64) {
	c.value.Add(v)
}

func (c *Counter) Inc() {
	c.Add(1)
}

func (c *Counter) Load() uint64 {
	return c.value.Load()
}

type Timer struct {
	count atomic.Uint64
	total atomic.Uint64
}

func (t *Timer) Observe(d time.Duration) {
	t.count.Add(1)
	t.total.Add(uint64(d.Nanoseconds()))
}

func (t *Timer) Count() uint64 {
	return t.count.Load()
}

func (t *Timer) Seconds() float64 {
	return float64(t.total.Load()) / float64(time.Second)
}

type StatusCounters struct {
	mu     sync.RWMutex
	values map[int]*Counter
}

func (s *StatusCounters) Inc(status int) {
	s.mu.Lock()
	if s.values == nil {
		s.values = map[int]*Counter{}
	}
	c := s.values[status]
	if c == nil {
		c = &Counter{}
		s.values[status] = c
	}
	s.mu.Unlock()
	c.Inc()
}

func (s *StatusCounters) Write(w io.Writer, name string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for status, c := range s.values {
		fmt.Fprintf(w, "%s{status=%q} %d\n", name, strconv.Itoa(status), c.Load())
	}
}

var (
	HTTPRequests                StatusCounters
	PlacementRequests           Counter
	PlacementErrors             Counter
	PlacementLatency            Timer
	SyncRounds                  Counter
	SyncRoundErrors             Counter
	SyncRoundLatency            Timer
	SyncRowsApplied             Counter
	SyncEventsApplied           Counter
	SyncChecksumMismatches      Counter
	SyncEventChecksumMismatches Counter
)
