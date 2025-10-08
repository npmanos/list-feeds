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

type CursorType string

const (
	PostCursor CursorType = "p"
	RepostCursor CursorType = "r"
	UnknownCursor CursorType = "u"
)

type FeedCursor struct {
	IndexedAt time.Time
	Type CursorType
	ID int64
}

func ParseCursor(cursorStr string) (*FeedCursor, error) {
	parts := strings.Split(cursorStr, "::")

	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid cursor format: %s", cursorStr)
	}

	idxInt, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("unable to parse IndexedAt in cursor: %w", err)
	}
	indexedAt := time.UnixMicro(int64(idxInt))

	cursorType := CursorType(parts[1])
	switch cursorType {
	case PostCursor, RepostCursor:
		break
	default:
		cursorType = UnknownCursor
	}

	id, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("unable to parse ID in cursor: %w", err)
	}

	return &FeedCursor{indexedAt, cursorType, int64(id)}, nil
}

func (fc *FeedCursor) MarshalJSON() ([]byte, error) {
	idxInt := fc.IndexedAt.UnixMicro()
	cursorStr := fmt.Sprintf("%d::%s::%d", idxInt, fc.Type, fc.ID)

	return json.Marshal(cursorStr)
}

func (fc *FeedCursor) UnmarshalJSON(b []byte) error {
	var cursorStr string
	if err := json.Unmarshal(b, &cursorStr); err != nil {
		return err
	}

	parsed, err := ParseCursor(string(b))
	if err != nil {
		return err
	}

	fc.IndexedAt = parsed.IndexedAt
	fc.Type = parsed.Type
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
	Post string `json:"post"`
	Reason *PostReason `json:"reason,omitzero"`
}

type FeedSkeleton struct {
	Cursor *FeedCursor `json:"cursor"`
	Feed []FeedPost `json:"feed"`
}

type FeedBuilder interface {
	BuildFeed(ctx context.Context, cursor string, limit int, db *bun.DB) *FeedSkeleton
}