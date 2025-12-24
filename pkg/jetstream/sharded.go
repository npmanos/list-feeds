package jetstream

import (
	"context"
	"fmt"
	"sync"
)

type ShardedJetstreamConsumer struct {
	consumers   []*JetstreamConsumer
	updates     <-chan *ListMemberUpdate
	updateChans []chan *ListMemberUpdate
}

func NewShardedJetstreamConsumer(
	config *JetstreamConfig,
) (*ShardedJetstreamConsumer, []string) {
	// Target ~150 DIDs per shard to prevent the subscription URL from exceeding
	// common server limits (usually 8KB). 150 DIDs * ~45 chars ≈ 6.7KB.
	const TargetShardSize = 150

	dids := config.WantedDids
	numShards := (len(dids) + TargetShardSize - 1) / TargetShardSize
	// Ensure at least a few shards if we expect growth, but for now just fit data
	if numShards < 1 {
		numShards = 1
	}

	// Distribute DIDs
	shardedDids := make([][]string, numShards)
	for i, did := range dids {
		shardIdx := i % numShards
		shardedDids[shardIdx] = append(shardedDids[shardIdx], did)
	}

	var consumers []*JetstreamConsumer
	var shardIDs []string
	var updateChans []chan *ListMemberUpdate

	for i := 0; i < numShards; i++ {
		shardID := fmt.Sprintf("%s-shard-%d", config.Name, i)
		shardIDs = append(shardIDs, shardID)

		shardConfig := *config
		shardConfig.Name = shardID
		shardConfig.ShardID = shardID
		shardConfig.WantedDids = shardedDids[i]

		updateChan := make(chan *ListMemberUpdate, 100)
		shardConfig.WantedDidsUpdates = updateChan
		updateChans = append(updateChans, updateChan)

		c := NewJetstreamConsumer(&shardConfig)
		consumers = append(consumers, c)
	}

	return &ShardedJetstreamConsumer{
		consumers:   consumers,
		updates:     config.WantedDidsUpdates,
		updateChans: updateChans,
	}, shardIDs
}

func (s *ShardedJetstreamConsumer) Start(ctx context.Context, wg *sync.WaitGroup) {
	for _, c := range s.consumers {
		wg.Add(1)
		go c.Start(ctx, wg)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if s.updates == nil {
			return
		}

		rrIndex := 0

		for {
			select {
			case <-ctx.Done():
				return
			case update := <-s.updates:
				if update.AddMember != "" {
					targetIdx := rrIndex % len(s.updateChans)
					s.updateChans[targetIdx] <- &ListMemberUpdate{AddMember: update.AddMember}
					rrIndex++
				}
				if update.RemoveMember != "" {
					// Broadcast remove to all shards
					for _, ch := range s.updateChans {
						ch <- &ListMemberUpdate{RemoveMember: update.RemoveMember}
					}
				}
			}
		}
	}()
}