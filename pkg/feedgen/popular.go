package feedgen

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/npmanos/list-feeds/pkg/config"
	persist "github.com/npmanos/list-feeds/pkg/db"
	"github.com/npmanos/list-feeds/pkg/utils"
	"github.com/uptrace/bun"
)

type PopularFeed struct {
	baseFeed
	FeedConfig *config.PopularFeedConfig
}

func NewPopularFeed(listURI string, serviceName string, maxLag time.Duration, feedConfig *config.PopularFeedConfig, db *bun.DB) *PopularFeed {
	return &PopularFeed {
		baseFeed: baseFeed{
			ListURI: listURI,
			ServiceName: serviceName,
			MaxLag: maxLag,
			db: db,
		},
		FeedConfig: feedConfig,
	}
}

type popularSelect struct {
	URI string `bun:"uri"`
	Score float64 `bun:"score"`
	Points int64 `bun:"points"`
	CreatedAt time.Time `bun:"created_at"`
}

func (f *PopularFeed) BuildFeed(ctx context.Context, cursor string, limit int) (*FeedSkeleton, error) {
	db := f.db
	weights := f.FeedConfig.Weights

	parsedCursor, err := ParseCursor(cursor)
	if err != nil {
		log.Printf("unable to parse cursor %s, discarding: %v", cursor, err)
	}

	listIdQ := db.NewSelect().Model((*persist.List)(nil)).Column("id").Where("uri = ?", f.ListURI)
	listMemberIdsQ := db.NewSelect().Model((*persist.ListToUser)(nil)).
		Column("user_id").
		Where("list_id = (?)", listIdQ)
	inMemberIds := db.NewSelect().Column("user_id").Table("list_member_ids")
	
	postPointsQ := db.NewSelect().
		Model((*persist.Post)(nil)).
		ColumnExpr("?TableAlias.id AS post_id").
		Column("uri", "created_at").
		ColumnExpr(
			"(COALESCE(likes.count, 0) * ? + COALESCE(reposts.count, 0) * ? + COALESCE(replies.count, 0) * ?) AS points",
			weights.Likes,
			weights.Reposts,
			weights.Replies,
		).ColumnExpr(
			"CASE WHEN ?TableAlias.author_id IN (?) THEN 1 ELSE 0 END AS is_list_member",
			inMemberIds,
		).Join(
			"LEFT JOIN (?) AS likes",
			f.pointsJoinQuery((*persist.Like)(nil), "post_id", "liker_id", inMemberIds),
		).JoinOn("?TableAlias.id = likes.post_id").
		Join(
			"LEFT JOIN (?) AS reposts",
			f.pointsJoinQuery((*persist.Repost)(nil), "post_id", "reposter_id", inMemberIds),
		).JoinOn("?TableAlias.id = reposts.post_id").
		Join(
			"LEFT JOIN (?) AS replies",
			f.pointsJoinQuery((*persist.Post)(nil), "reply_parent_id", "author_id", inMemberIds).
				Where("reply_parent_id IS NOT NULL"),
		).JoinOn("?TableAlias.id = replies.reply_parent_id")
	
	rankedPostsQ := db.NewSelect().
		Column("post_points.uri", "post_points.points", "post_points.created_at").
		ColumnExpr(
			`(?0.?1) / POW(
				(unixepoch('now') - unixepoch(?0.?2)) / 3600.0 + 2,
				?3
			) * CASE WHEN ?0.?4 = 1 THEN ?5 ELSE 1.0 END AS score`,
			bun.Ident("post_points"),
			bun.Ident("points"),
			bun.Ident("created_at"),
			weights.Gravity,
			bun.Ident("is_list_member"),
			weights.MemberMultiplier,
		).Table("post_points").
		Where("?.? > 0", bun.Ident("post_points"), bun.Ident("points"))
	
	
	var posts []popularSelect
	
	query := db.NewSelect().
		With("list_member_ids", listMemberIdsQ).
		With("post_points", postPointsQ).
		With("ranked_posts", rankedPostsQ).
		Column("uri", "score", "points", "created_at").
		Table("ranked_posts").
		Where("created_at > datetime('now', '-' || ? || ' hours')", f.FeedConfig.MaxAgeHours).
		Order("score DESC", "created_at DESC").
		Limit(limit)
	
	if parsedCursor != nil {
		cursorScore, err := strconv.ParseFloat(parsedCursor.ID, 64)
		if err != nil {
			log.Printf("unable to parse cursor %s, discarding: %v", cursor, err)
		} else {
			query = query.WhereGroup(" AND ", func(sq *bun.SelectQuery) *bun.SelectQuery {
				return sq.Where("score < ?", cursorScore).
					WhereGroup(" OR ", func(sq *bun.SelectQuery) *bun.SelectQuery {
						return sq.Where("score = ?", cursorScore).Where("created_at < ?", parsedCursor.CreatedAt)
					})
			})
		}
	}
	
	if err := query.Scan(ctx, &posts); err != nil {
		return nil, fmt.Errorf("unable to get posts to populate %s: %w", f.FeedConfig.Slug, err)
	}

	feedItems, _ := utils.Map(posts, func(fs popularSelect) (FeedPost, error) {
		return FeedPost{PostURI: fs.URI}, nil
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

	var newCursor FeedCursor
	if len(posts) > 0 {
		last_post := posts[len(posts) - 1]
		newCursor.CreatedAt = last_post.CreatedAt
		newCursor.ID = strconv.FormatFloat(last_post.Score, 'f', -1, 64)
	}

	return &FeedSkeleton{Cursor: &newCursor, Feed: feedItems}, nil
}

func (f *PopularFeed) pointsJoinQuery(model interface{}, col string, whereCol string, memberIdsSubq *bun.SelectQuery) *bun.SelectQuery {
	return f.db.NewSelect().
		Model(model).
		Column(col).
		ColumnExpr("COUNT(*) AS count").
		Where("? IN (?)", bun.Ident(whereCol), memberIdsSubq).
		Group(col)
}