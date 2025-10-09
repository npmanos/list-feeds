package feedgen

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/uptrace/bun"
)

type FeedCursor struct {
	CreatedAt time.Time
	ID string
}

func ParseCursor(cursorStr string) (*FeedCursor, error) {
	if cursorStr == "" {
		return nil, nil
	}

	parts := strings.Split(cursorStr, "::")

	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid cursor format: %s", cursorStr)
	}

	createdInt, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("unable to parse CreatedAt in cursor: %w", err)
	}
	createdAt := time.UnixMilli(int64(createdInt))

	return &FeedCursor{createdAt, parts[1]}, nil
}

func (fc *FeedCursor) MarshalJSON() ([]byte, error) {
	idxInt := fc.CreatedAt.UnixMilli()
	cursorStr := fmt.Sprintf("%d::%s", idxInt, fc.ID)

	return json.Marshal(cursorStr)
}

func (fc *FeedCursor) UnmarshalJSON(b []byte) error {
	var cursorStr string
	if err := json.Unmarshal(b, &cursorStr); err != nil {
		return err
	}

	parsed, err := ParseCursor(cursorStr)
	if err != nil {
		return err
	}

	fc.CreatedAt = parsed.CreatedAt
	fc.ID = parsed.ID

	return nil
}

type SkeletonReason string

const (
	ReasonRepost SkeletonReason = "app.bsky.feed.defs#skeletonReasonRepost"
	ReasonPin SkeletonReason = "app.bsky.feed.defs#skeletonReasonPin"
)

type PostReason struct {
	Type SkeletonReason `json:"$type"`
	Repost string `json:"repost,omitzero"`
}

type FeedPost struct {
	PostURI string `json:"post"`
	Reason *PostReason `json:"reason,omitzero"`
}

type FeedSkeleton struct {
	Cursor *FeedCursor `json:"cursor"`
	Feed []FeedPost `json:"feed"`
}

type FeedBuilder interface {
	BuildFeed(ctx context.Context, cursor string, limit int, db *bun.DB) (*FeedSkeleton, error)
}

type FeedURI struct {
	URI string `json:"uri"`
}

type FeedGenDescription struct {
	DID string `json:"did"`
	Feeds []FeedURI `json:"feeds"`
}