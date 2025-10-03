package main

import (
	"context"
	"log"

	"github.com/npmanos/list-feeds/pkg/config"
	"github.com/npmanos/list-feeds/pkg/db"
	"github.com/npmanos/list-feeds/pkg/db/migrations"
	"github.com/uptrace/bun/migrate"
)

func main() {
	log.Println("Starting application")

	cfg, err := config.LoadConfig(config.DEFAULT_CONFIG_FILE)
	if err != nil {
		log.Fatalf("failed to load config %v", err)
	}
	log.Println("Loaded configuration")

	db, err := db.Initialize(cfg)
	if err != nil {
		log.Fatalf("failed to initialize database: %v", err)
	}
	log.Println("Database initialized")

	ctx := context.Background()
	migrator := migrate.NewMigrator(db, migrations.Migrations)

	if err := migrator.Init(ctx); err != nil {
		log.Fatalf("failed to initialize migrations: %v", err)
	}
	log.Println("Migrations initialized")

	group, err := migrator.Migrate(ctx)
	if err != nil {
		log.Fatalf("migration failed: %v", err)
	}

	if group.IsZero() {
		log.Println("No new migrations to apply")
	} else {
		log.Printf("Applied migrations: %s", group)
	}

	log.Println("Application ready")
}
