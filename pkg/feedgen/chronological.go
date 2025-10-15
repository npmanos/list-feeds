package feedgen

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/npmanos/list-feeds/pkg/config"
	persist "github.com/npmanos/list-feeds/pkg/db"
	"github.com/npmanos/list-feeds/pkg/utils"
	"github.com/uptrace/bun"
)

type ChronologicalFeed struct {
	baseFeed
	FeedConfig *config.ChronologicalFeedConfig
}

func NewChronologicalFeed(listURI string, serviceName string, maxLag time.Duration, feedConfig *config.ChronologicalFeedConfig, db *bun.DB) *ChronologicalFeed {
	return &ChronologicalFeed{
		baseFeed: baseFeed{
			ListURI: listURI,
			ServiceName: serviceName,
			MaxLag: maxLag,
			db:      db,
		},
		FeedConfig: feedConfig,
	}
}

type chronSelect struct {
	CID string `bun:"cid"`
	URI string `bun:"uri"`
	CreatedAt time.Time `bun:"created_at"`
	RepostURI string `bun:"repost_uri,nullzero"`
}

func (f *ChronologicalFeed) BuildFeed(ctx context.Context, cursor string, limit int) (*FeedSkeleton, error) {
	db := f.db

	parsedCursor, err := ParseCursor(cursor)
	if err != nil {
		log.Printf("unable to parse cursor %s, discarding: %v", cursor, err)
	}

	list := new(persist.List)
	if err := db.NewSelect().Model(list).Relation("ListMembers").Where("uri = ?", f.ListURI).Scan(ctx); err != nil {
		return nil, fmt.Errorf("unable to build chronological feed %s: %w", f.FeedConfig.Slug, err)
	}

	userIds, _ := utils.Map(list.ListMembers, func(u persist.User) (int64, error) { return u.ID, nil })

	authoredSelect := db.NewSelect().Model((*persist.Post)(nil)).
		Join("LEFT JOIN ?TableName AS reply_parent ON reply_parent.id = ?TableAlias.reply_parent_id").
		Column("cid", "uri", "created_at").
		ColumnExpr("NULL AS repost_uri").
		Where("?TableAlias.author_id IN (?)", bun.In(userIds)).
		WhereGroup(" AND ", func(sq *bun.SelectQuery) *bun.SelectQuery {
			return sq.Where("?TableAlias.reply_parent_id IS NULL").
				WhereOr("reply_parent.author_id IN (?)", bun.In(userIds))
		})
	
	repostSelect := db.NewSelect().Model((*persist.Repost)(nil)).
		Join("LEFT JOIN posts AS post ON post.id = ?TableAlias.post_id").
		ColumnExpr("post.cid AS cid").
		ColumnExpr("post.uri AS uri").
		ColumnExpr("?TableAlias.created_at AS created_at").
		ColumnExpr("?TableAlias.uri AS repost_uri").
		Where("reposter_id IN (?)", bun.In(userIds))
	
	var posts []chronSelect
	query := db.NewSelect().
		With("feed_posts", db.NewRaw("? UNION ALL ?", authoredSelect, repostSelect)).
		Table("feed_posts").
		Order("created_at DESC", "cid DESC").
		Limit(limit)
	
	if parsedCursor != nil {
		query = query.Where("created_at < ?", parsedCursor.CreatedAt).
			WhereGroup(" OR ", func(sq *bun.SelectQuery) *bun.SelectQuery {
				return sq.Where("created_at = ?", parsedCursor.CreatedAt).
					Where("cid < ?", parsedCursor.ID)
			})
	}


	if err := query.Scan(ctx, &posts); err != nil {
		return nil, fmt.Errorf("unable to get posts to populate %s: %w", f.FeedConfig.Slug, err)
	}
	
	feedItems, _ := utils.Map(posts, func(fs chronSelect) (FeedPost, error) {
		var fp = FeedPost{PostURI: fs.URI}
		if fs.RepostURI != "" {
			fp.Reason = &PostReason{Type: ReasonRepost, Repost: fs.RepostURI}
		}

		return fp, nil
	})

	if f.FeedConfig.LagNoticePost != "" {
		if lag, err := persist.GetLag(ctx, f.ServiceName, f.db); err != nil || lag > f.MaxLag {
			lagPost := FeedPost{
				PostURI: f.FeedConfig.LagNoticePost,
				Reason: &PostReason{
					Type: ReasonPin,
				},
			}
			feedItems = utils.Prepend(feedItems, lagPost)
		}
	}

	lastPost := posts[len(posts)-1]
	newCursor := FeedCursor{CreatedAt: lastPost.CreatedAt, ID: lastPost.CID}

	return &FeedSkeleton{Cursor: &newCursor, Feed: feedItems}, nil
}