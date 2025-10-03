package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/npmanos/list-feeds/pkg/config"
	"github.com/npmanos/list-feeds/pkg/db"
	"github.com/npmanos/list-feeds/pkg/db/migrations"
	"github.com/npmanos/list-feeds/pkg/jetstream"
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

	ctx, cancel := context.WithCancel(context.Background())
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

	postConsumer := jetstream.NewJetstreamConsumer(
		"Post consumer",
		cfg.JetstreamHosts,
		1,
		[]string{},
		jetstream.POST_COLLECTIONS,
		0,
		map[string]string{},
	)

	listChangeConsumer := jetstream.NewJetstreamConsumer(
		"List change consumer",
		cfg.JetstreamHosts,
		1,
		[]string{},
		jetstream.LIST_MEMBER_COLLECTIONS,
		0,
		map[string]string{},
	)

	log.Println("Running... Press Ctrl+C to exit.")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	wg := new(sync.WaitGroup)

	wg.Add(1)
	go postConsumer.Start(ctx, wg)
	wg.Add(1)
	go listChangeConsumer.Start(ctx, wg)

	<-quit
	log.Println("Shutting down...")
	cancel()

	wg.Wait()
}
