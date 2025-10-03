package db

import (
	"database/sql"
	"fmt"

	"github.com/npmanos/list-feeds/pkg/config"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/driver/sqliteshim"
	"github.com/uptrace/bun/extra/bundebug"
)

func Initialize(cfg *config.Config) (*bun.DB, error) {
	var sqldb *sql.DB
	var debug bool
	var err error

	switch c := cfg.DbConfig.(type) {
	case *config.SqliteConfig:
		sqldb, err = sql.Open(sqliteshim.ShimName, "file:"+c.Path+"?cache=shared&_journal_mode=WAL&_busy_timeout=5000")
		if err != nil {
			return nil, err
		}

		sqldb.SetMaxOpenConns(1)
		sqldb.SetMaxIdleConns(1)
		sqldb.SetConnMaxLifetime(0)
		sqldb.SetConnMaxIdleTime(0)

		debug = c.Debug

	default:
		return nil, fmt.Errorf("unknown database config type: %T", c)
	}

	if err := sqldb.Ping(); err != nil {
		return nil, err
	}

	db := bun.NewDB(sqldb, sqlitedialect.New())
	db.AddQueryHook(bundebug.NewQueryHook(bundebug.WithVerbose(debug)))
	db.RegisterModel((*ListToUser)(nil))

	return db, nil
}
