package feedgen

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/npmanos/list-feeds/pkg/config"
	persist "github.com/npmanos/list-feeds/pkg/db"
	"github.com/npmanos/list-feeds/pkg/utils"
	"github.com/uptrace/bun"
)

type ChronologicalFeed struct {
	ListURI string
	FeedConfig *config.ChronologicalFeedConfig
}

type feedSelect struct {
	CID string `bun:"cid"`
	URI string `bun:"uri"`
	CreatedAt time.Time `bun:"created_at"`
	RepostURI string `bun:"repost_uri,nullzero"`
}

func (f *ChronologicalFeed) BuildFeed(ctx context.Context, cursor string, limit int, db *bun.DB) (*FeedSkeleton, error) {
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
	
	var posts []feedSelect
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
	
	feedItems, _ := utils.Map(posts, func(fs feedSelect) (FeedPost, error) {
		var fp = FeedPost{PostURI: fs.URI}
		if fs.RepostURI != "" {
			fp.Reason = &PostReason{Type: ReasonRepost, Repost: fs.RepostURI}
		}

		return fp, nil
	})

	lastPost := posts[len(posts)-1]
	newCursor := FeedCursor{CreatedAt: lastPost.CreatedAt, ID: lastPost.CID}

	skeleton := FeedSkeleton{Cursor: &newCursor, Feed: feedItems}
	json, err := json.Marshal(skeleton)
	log.Printf("%v, %v", string(json), err)

	return &skeleton, nil
}