package migrations

import (
	"context"

	_db "github.com/npmanos/list-feeds/pkg/db"
	"github.com/uptrace/bun"
)

func init() {
	Migrations.MustRegister(func(ctx context.Context, db *bun.DB) error {
		_, err := db.NewDropColumn().Model((*_db.SubscriptionState)(nil)).Column("lag").Exec(ctx)
		if err != nil {
			return err
		}

		_, err = db.NewAddColumn().Model((*_db.SubscriptionState)(nil)).ColumnExpr("lag INTEGER NOT NULL DEFAULT 0").Exec(ctx)
		return err
	}, func(ctx context.Context, db *bun.DB) error {
		_, err := db.NewDropColumn().Model((*_db.SubscriptionState)(nil)).Column("lag").Exec(ctx)
		if err != nil {
			return err
		}

		_, err = db.NewAddColumn().Model((*_db.SubscriptionState)(nil)).ColumnExpr("lag TIMESTAMP NOT NULL DEFAULT current_timestamp").Exec(ctx)
		return err
	})
}