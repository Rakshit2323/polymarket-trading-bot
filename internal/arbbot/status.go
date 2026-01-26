package arbbot

import (
	"log"
	"time"
)

type statusSlot struct {
	msg    string
	lastAt time.Time
}

type statusTracker struct {
	prefix      string
	minInterval time.Duration
	slots       map[string]statusSlot
}

func newStatusTracker(prefix string, minInterval time.Duration) statusTracker {
	if minInterval < 0 {
		minInterval = 0
	}
	return statusTracker{
		prefix:      prefix,
		minInterval: minInterval,
		slots:       make(map[string]statusSlot),
	}
}

func (s *statusTracker) Set(slot, msg string) {
	if s == nil || slot == "" || msg == "" {
		return
	}
	if s.slots == nil {
		s.slots = make(map[string]statusSlot)
	}
	now := time.Now()
	prev := s.slots[slot]
	if prev.msg == msg && !prev.lastAt.IsZero() && now.Sub(prev.lastAt) < s.minInterval {
		return
	}
	s.slots[slot] = statusSlot{msg: msg, lastAt: now}
	log.Printf("%s status %s=%s", s.prefix, slot, msg)
}
