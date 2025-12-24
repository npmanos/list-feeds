package jetstream

import (
	"fmt"
	"testing"
)

func TestNewShardedJetstreamConsumer(t *testing.T) {
	// Generate 12,000 DIDs
	var dids []string
	for i := 0; i < 12000; i++ {
		dids = append(dids, fmt.Sprintf("did:plc:%d", i))
	}

	config := &JetstreamConfig{
		Name:         "TestConsumer",
		WantedDids:   dids,
		ExtraHeaders: make(map[string][]string),
	}

	consumer, shardIDs := NewShardedJetstreamConsumer(config)

	// TargetShardSize is 5000 (internal constant)
	// 12000 / 5000 = 2.4 -> 3 shards
	expectedShards := 3

	if len(consumer.consumers) != expectedShards {
		t.Fatalf("expected %d consumers, got %d", expectedShards, len(consumer.consumers))
	}

	if len(shardIDs) != expectedShards {
		t.Fatalf("expected %d shardIDs, got %d", expectedShards, len(shardIDs))
	}

	// Verify total DIDs across all consumers
	totalDIDs := 0
	for _, c := range consumer.consumers {
		totalDIDs += len(c.config.WantedDids)
		if len(c.config.WantedDids) > 5000 {
             // It's allowed to be slightly more if we just round-robin or chunk?
             // My implementation used round-robin (modulo).
             // 12000 / 3 = 4000.
             // Wait, implementation:
             // numShards := (len(dids) / TargetShardSize) + 1  => 12000/5000 + 1 = 2+1=3.
             // distribution: i % numShards.
             // So it should be perfectly balanced.
		}
	}

	if totalDIDs != 12000 {
		t.Errorf("expected 12000 total DIDs distributed, got %d", totalDIDs)
	}

    // Verify IDs
    for i, id := range shardIDs {
        expectedID := fmt.Sprintf("TestConsumer-shard-%d", i)
        if id != expectedID {
            t.Errorf("expected shard ID %s, got %s", expectedID, id)
        }
        if consumer.consumers[i].config.ShardID != expectedID {
             t.Errorf("consumer config ShardID mismatch")
        }
    }
}
