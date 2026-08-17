package model

import (
	"sync"
	"time"
)

type HLC struct {
	TS  int64
	Seq int64
}

func (h HLC) Compare(o HLC) int {
	if h.TS != o.TS {
		if h.TS < o.TS {
			return -1
		}
		return 1
	}
	if h.Seq == o.Seq {
		return 0
	}
	if h.Seq < o.Seq {
		return -1
	}
	return 1
}

func (h HLC) After(o HLC) bool {
	return h.Compare(o) > 0
}

type Clock struct {
	mu  sync.Mutex
	ts  int64
	seq int64
	now func() int64
}

func NewClock(now func() int64) *Clock {
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	return &Clock{now: now}
}

func (c *Clock) Tick() HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := c.now(); n > c.ts {
		c.ts = n
		c.seq = 0
	} else {
		c.seq++
	}
	return HLC{TS: c.ts, Seq: c.seq}
}

func (c *Clock) Update(o HLC) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if o.TS > c.ts {
		c.ts = o.TS
		c.seq = o.Seq
	} else if o.TS == c.ts && o.Seq > c.seq {
		c.seq = o.Seq
	}
}
