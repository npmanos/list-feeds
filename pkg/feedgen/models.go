package feedgen

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
	Cursor string `json:"cursor"`
	Feed []FeedPost `json:"feed"`
}