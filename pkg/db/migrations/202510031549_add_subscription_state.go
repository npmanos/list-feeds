package migrations

import (
	"context"

	_db "github.com/npmanos/list-feeds/pkg/db"
	"github.com/uptrace/bun"
)

func init() {
	Migrations.MustRegister(func(ctx context.Context, db *bun.DB) error {
		_, err := db.NewCreateTable().IfNotExists().Model((*_db.SubscriptionState)(nil)).Exec(ctx)
		return err
	}, func(ctx context.Context, db *bun.DB) error {
		_, err := db.NewDropTable().Model((*_db.SubscriptionState)(nil)).Exec(ctx)
		return err
	})
}
