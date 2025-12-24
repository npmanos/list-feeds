package db

import (
	"container/heap"

	"github.com/npmanos/list-feeds/pkg/jetstream"
)

type EventHeap []*jetstream.Event

func (h EventHeap) Len() int           { return len(h) }
func (h EventHeap) Less(i, j int) bool { return h[i].Cursor < h[j].Cursor }
func (h EventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *EventHeap) Push(x any)        { *h = append(*h, x.(*jetstream.Event)) }
func (h *EventHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
func (h EventHeap) Peek() *jetstream.Event {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}

type Sequencer struct {
	eventHeap    *EventHeap
	shardCursors map[string]int64
	shardIDs     []string
	minCursor    int64
}

func NewSequencer(shardIDs []string) *Sequencer {
	h := &EventHeap{}
	heap.Init(h)
	
	return &Sequencer{
		eventHeap:    h,
		shardCursors: make(map[string]int64),
		shardIDs:     shardIDs,
		minCursor:    0,
	}
}

func (s *Sequencer) Push(event *jetstream.Event) {
	if event.ShardID != "" {
		s.shardCursors[event.ShardID] = event.Cursor
	}

	if event.Kind != jetstream.HeartbeatEvent {
		heap.Push(s.eventHeap, event)
	}

	s.recomputeMin()
}

func (s *Sequencer) recomputeMin() {
	var min int64 = -1

	for _, id := range s.shardIDs {
		val := s.shardCursors[id] // Returns 0 if not present
		if min == -1 || val < min {
			min = val
		}
	}

	if min == -1 {
		min = 0
	}
	s.minCursor = min
}

func (s *Sequencer) Pop() *jetstream.Event {
	if s.eventHeap.Len() == 0 {
		return nil
	}
	// Peek
	top := (*s.eventHeap)[0]
	if top.Cursor <= s.minCursor {
		return heap.Pop(s.eventHeap).(*jetstream.Event)
	}
	return nil
}
