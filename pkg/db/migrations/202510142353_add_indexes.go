package migrations

import (
	"context"

	_db "github.com/npmanos/list-feeds/pkg/db"
	"github.com/uptrace/bun"
)

func init() {
	Migrations.MustRegister(func(ctx context.Context, db *bun.DB) error {
		post := (*_db.Post)(nil)
		likes := (*_db.Like)(nil)
		reposts := (*_db.Repost)(nil)
		createIndexes := []bun.CreateIndexQuery {
			*db.NewCreateIndex().Model(post).IfNotExists().Index("idx_posts_author_id").Column("author_id"),
			*db.NewCreateIndex().Model(post).IfNotExists().Index("idx_posts_reply_parent_id").Column("reply_parent_id"),
			*db.NewCreateIndex().Model(post).IfNotExists().Index("idx_posts_reply_root_id").Column("reply_root_id"),
			*db.NewCreateIndex().Model(post).IfNotExists().Index("idx_posts_created_at").Column("created_at"),

			*db.NewCreateIndex().Model(likes).IfNotExists().Index("idx_likes_liker_id").Column("liker_id"),
			*db.NewCreateIndex().Model(likes).IfNotExists().Index("idx_likes_post_id").Column("post_id"),
			*db.NewCreateIndex().Model(likes).IfNotExists().Index("idx_likes_created_at").Column("created_at"),

			*db.NewCreateIndex().Model(reposts).IfNotExists().Index("idx_reposts_reposter_id").Column("reposter_id"),
			*db.NewCreateIndex().Model(reposts).IfNotExists().Index("idx_reposts_post_id").Column("post_id"),
			*db.NewCreateIndex().Model(reposts).IfNotExists().Index("idx_reposts_created_at").Column("created_at"),
		}

		for _, idx := range createIndexes {
			if _, err := idx.Exec(ctx); err != nil {
				return err
			}
		}

		return nil
	}, func(ctx context.Context, db *bun.DB) error {
		dropIndexes := []bun.DropIndexQuery {
			*db.NewDropIndex().IfExists().Index("idx_posts_author_id"),
			*db.NewDropIndex().IfExists().Index("idx_posts_reply_parent_id"),
			*db.NewDropIndex().IfExists().Index("idx_posts_reply_root_id"),
			*db.NewDropIndex().IfExists().Index("idx_posts_created_at"),

			*db.NewDropIndex().IfExists().Index("idx_likes_liker_id"),
			*db.NewDropIndex().IfExists().Index("idx_likes_post_id"),
			*db.NewDropIndex().IfExists().Index("idx_likes_created_at"),

			*db.NewDropIndex().IfExists().Index("idx_reposts_reposter_id"),
			*db.NewDropIndex().IfExists().Index("idx_reposts_post_id"),
			*db.NewDropIndex().IfExists().Index("idx_reposts_created_at"),
		}

		for _, idx := range dropIndexes {
			if _, err := idx.Exec(ctx); err != nil {
				return err
			}
		}

		return nil
	})
}