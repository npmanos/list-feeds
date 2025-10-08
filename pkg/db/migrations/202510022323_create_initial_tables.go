package migrations

import (
	"context"
	"slices"

	"github.com/npmanos/list-feeds/pkg/db"
	"github.com/uptrace/bun"
)

var models = []interface{}{
	(*db.User)(nil),
	(*db.List)(nil),
	(*db.Post)(nil),
	(*db.Repost)(nil),
	(*db.Like)(nil),
	(*db.ListToUser)(nil),
}

func init() {
	Migrations.MustRegister(func(ctx context.Context, db *bun.DB) error {
		for _, model := range models {
			_, err := db.NewCreateTable().
				Model(model).
				IfNotExists().
				WithForeignKeys().
				Exec(ctx)

			if err != nil {
				return err
			}
		}

		return nil
	}, func(ctx context.Context, db *bun.DB) error {
		slices.Reverse(models)
		for _, model := range models {
			_, err := db.NewDropTable().
				Model(model).
				Exec(ctx)

			if err != nil {
				return err
			}
		}

		return nil
	})
}
