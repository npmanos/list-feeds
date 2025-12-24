package db

import (
	"container/heap"
	"testing"

	"github.com/npmanos/list-feeds/pkg/jetstream"
)

func TestEventHeap(t *testing.T) {
	h := &EventHeap{}
	heap.Init(h)

	events := []*jetstream.Event{
		{Cursor: 100},
		{Cursor: 50},
		{Cursor: 150},
		{Cursor: 10},
		{Cursor: 200},
	}

	for _, e := range events {
		heap.Push(h, e)
	}

	if h.Len() != 5 {
		t.Fatalf("expected heap length 5, got %d", h.Len())
	}

	expectedCursors := []int64{10, 50, 100, 150, 200}
	for i, expected := range expectedCursors {
		e := heap.Pop(h).(*jetstream.Event)
		if e.Cursor != expected {
			t.Errorf("expected cursor %d at index %d, got %d", expected, i, e.Cursor)
		}
	}
}
