package db

import (
	"testing"

	"github.com/npmanos/list-feeds/pkg/jetstream"
)

func TestSequencerOrdering(t *testing.T) {
	shardIDs := []string{"shardA", "shardB"}
	seq := NewSequencer(shardIDs)

	// Helper to push event
	push := func(shardID string, cursor int64, kind jetstream.EventKind) {
		seq.Push(&jetstream.Event{
			ShardID: shardID,
			Cursor:  cursor,
			Kind:    kind,
		})
	}

	// 1. Initial state: cursors at 0. Min=0.
	if e := seq.Pop(); e != nil {
		t.Errorf("expected nil pop, got %v", e)
	}

	// 2. Push A1 (t=100). A=100, B=0. Min=0.
	push("shardA", 100, jetstream.CommitEvent)
	if e := seq.Pop(); e != nil {
		t.Errorf("expected nil pop after A1, got %v", e)
	}

	// 3. Push B1 (t=50). A=100, B=50. Min=50.
	// B1 (50) <= 50. Should pop B1.
	push("shardB", 50, jetstream.CommitEvent)
	
	e1 := seq.Pop()
	if e1 == nil || e1.Cursor != 50 || e1.ShardID != "shardB" {
		t.Errorf("expected B1 (50), got %v", e1)
	}

	// Heap has A1(100). Min=50. A1 > 50. No pop.
	if e := seq.Pop(); e != nil {
		t.Errorf("expected nil pop after B1, got %v", e)
	}

	// 4. Push Heartbeat B (t=120). A=100, B=120. Min=100.
	// A1 (100) <= 100. Should pop A1.
	push("shardB", 120, jetstream.HeartbeatEvent)

	e2 := seq.Pop()
	if e2 == nil || e2.Cursor != 100 || e2.ShardID != "shardA" {
		t.Errorf("expected A1 (100), got %v", e2)
	}

	// 5. Push A2 (t=150). A=150, B=120. Min=120.
	push("shardA", 150, jetstream.CommitEvent)
	if e := seq.Pop(); e != nil {
		t.Errorf("expected nil pop after A2, got %v", e)
	}

	// 6. Push Heartbeat B (t=200). A=150, B=200. Min=150.
	// A2 (150) <= 150. Pop A2.
	push("shardB", 200, jetstream.HeartbeatEvent)
	e3 := seq.Pop()
	if e3 == nil || e3.Cursor != 150 || e3.ShardID != "shardA" {
		t.Errorf("expected A2 (150), got %v", e3)
	}
}