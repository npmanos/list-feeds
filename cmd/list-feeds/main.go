package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/npmanos/list-feeds/pkg/atpclient"
	"github.com/npmanos/list-feeds/pkg/config"
	persist "github.com/npmanos/list-feeds/pkg/db"
	"github.com/npmanos/list-feeds/pkg/db/migrations"
	"github.com/npmanos/list-feeds/pkg/jetstream"
	"github.com/npmanos/list-feeds/pkg/server"
	"github.com/npmanos/list-feeds/pkg/utils"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/migrate"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
)

func main() {
	log.Println("Starting application")

	cfg, err := config.LoadConfig(config.DEFAULT_CONFIG_FILE)
	if err != nil {
		log.Fatalf("failed to load config %v", err)
	}
	log.Println("Loaded configuration")

	db, err := persist.Initialize(cfg)
	if err != nil {
		log.Fatalf("failed to initialize database: %v", err)
	}
	defer db.Close()
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

	serviceName, err := initSubState(ctx, cfg.ServiceConfig, db)
	if err != nil {
		log.Fatalf("unable to initialize subcription state: %v", err)
	}

	if err := syncLists(ctx, cfg.ListFeedConfigs, db); err != nil {
		log.Fatalf("list sync failed: %v", err)
	}

	memberDids, err := refreshLists(ctx, cfg.ListFeedConfigs, db)
	if err != nil {
		log.Fatalf("list member sync failed: %v", err)
	}

	listOwnerDids, err := utils.Map(cfg.ListFeedConfigs, func(lc config.ListFeedConfig) (string, error) { return lc.ListDID() })
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Application ready")

	wg := new(sync.WaitGroup)

	dbTxs := make(chan persist.TxFn)
	wg.Add(1)
	go persist.StartDbWriter(ctx, db, dbTxs, wg)
	wg.Add(1)
	go persist.StartDbJanitor(ctx, cfg.ServiceConfig.MaxAgeDays, dbTxs, db, wg)

	postOpEvents := make(chan *jetstream.Event)

	subState := persist.SubscriptionState{Service: serviceName}
	cursor, err := subState.GetCursor(ctx, db)
	if err != nil {
		log.Fatalf("failed to load cursor from db: %v", err)
	}

	if cursor > 1 {
		log.Printf("Resuming from Jetstream cursor %d", cursor)
	}

	didUpdates := make(chan *jetstream.ListMemberUpdate)

	postConsumer, shardIDs := jetstream.NewShardedJetstreamConsumer(&jetstream.JetstreamConfig{
		Name:              "Post consumer",
		Hosts:             cfg.JetstreamHosts,
		Cursor:            cursor,
		WantedDids:        memberDids,
		WantedCollections: jetstream.POST_COLLECTIONS,
		MaxSize:           0,
		ExtraHeaders:      http.Header{},
		EventsChannel:     postOpEvents,
		WantedDidsUpdates: didUpdates,
	})

	wg.Add(1)
	go persist.StartPostOpPersister(ctx, serviceName, postOpEvents, dbTxs, wg, shardIDs)

	listMemberEvents := make(chan *jetstream.Event)
	wg.Add(1)
	go persist.StartListMemberPersister(
		ctx,
		cfg.JetstreamHosts,
		listMemberEvents,
		postOpEvents,
		didUpdates,
		dbTxs,
		wg,
	)

	listChangeConsumer := jetstream.NewJetstreamConsumer(&jetstream.JetstreamConfig{
		Name:              "List change consumer",
		Hosts:             cfg.JetstreamHosts,
		Cursor:            cursor,
		WantedDids:        listOwnerDids,
		WantedCollections: jetstream.LIST_MEMBER_COLLECTIONS,
		MaxSize:           0,
		ExtraHeaders:      http.Header{},
		EventsChannel:     listMemberEvents,
	})

	log.Println("Running... Press Ctrl+C to exit.")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	wg.Add(1)
	go postConsumer.Start(ctx, wg)
	wg.Add(1)
	go listChangeConsumer.Start(ctx, wg)

	srv := server.NewServer(cfg, db)
	httpServer := & http.Server{
		Addr: net.JoinHostPort("0.0.0.0", "7474"),
		Handler: srv,
	}

	go func ()  {
		log.Printf("Listening on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("error listening and serving: %v", err)
			cancel()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<- ctx.Done()
		shutdownCtx := context.Background()
		shutdownCtx, serverShutdownCancel := context.WithTimeout(shutdownCtx, 10 * time.Second)
		defer serverShutdownCancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			fmt.Printf("error shutting down http server: %v", err)
		}
	}()

	<-quit
	log.Println("Shutting down...")
	cancel()

	wg.Wait()
}

func syncLists(ctx context.Context, listConfigs []config.ListFeedConfig, db *bun.DB) error {
	log.Println("Syncing lists with config file...")

	// 1. Get all list URIs from the config file into a map for easy lookup.
	configListURIs := make(map[string]struct{})
	for _, lc := range listConfigs {
		configListURIs[lc.ListURI] = struct{}{}
	}

	// 2. Get all list URIs and IDs currently in the database.
	var dbLists []persist.List
	err := db.NewSelect().Model(&dbLists).Column("id", "uri").Scan(ctx)
	if err != nil {
		return fmt.Errorf("failed to select lists from db: %w", err)
	}

	dbListMap := make(map[string]int64)
	for _, l := range dbLists {
		dbListMap[l.URI] = l.ID
	}

	// 3. Determine which lists to add and which to delete.
	var listsToAdd []persist.List
	var listIDsToDelete []int64

	for uri := range configListURIs {
		if _, found := dbListMap[uri]; !found {
			listsToAdd = append(listsToAdd, persist.List{URI: uri})
		}
	}

	for uri, id := range dbListMap {
		if _, found := configListURIs[uri]; !found {
			listIDsToDelete = append(listIDsToDelete, id)
		}
	}

	// 4. Execute the changes in a single transaction.
	return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if len(listsToAdd) > 0 {
			log.Printf("Adding %d new list(s) to the database", len(listsToAdd))
			_, err := tx.NewInsert().Model(&listsToAdd).Exec(ctx)
			if err != nil {
				return err
			}
		}

		if len(listIDsToDelete) > 0 {
			log.Printf("Removing %d stale list(s) from the database", len(listIDsToDelete))
			// First, delete all relationships from the 'list_members' join table.
			_, err := tx.NewDelete().Model((*persist.ListToUser)(nil)).
				Where("list_id IN (?)", bun.In(listIDsToDelete)).
				Exec(ctx)
			if err != nil {
				return err
			}

			// Then, delete the lists themselves from the 'lists' table.
			_, err = tx.NewDelete().Model((*persist.List)(nil)).
				Where("id IN (?)", bun.In(listIDsToDelete)).
				Exec(ctx)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func refreshLists(ctx context.Context, listConfigs []config.ListFeedConfig, db *bun.DB) ([]string, error) {
	apiClient := atpclient.GetATProtoClient()
	allApiMembers := make(map[string]*persist.ListToUser)

	err := db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		for _, listConfig := range listConfigs {
			log.Printf("Syncing members for list %s", listConfig.ListURI)

			var list persist.List
			err := tx.NewSelect().
				Model(&list).
				Where("uri = ?", listConfig.ListURI).
				Scan(ctx)

			if err != nil && err != sql.ErrNoRows {
				return err
			}

			list.URI = listConfig.ListURI

			var cursor string
			for {
				listMembers, err := appbsky.GraphGetList(ctx, apiClient, cursor, 100, listConfig.ListURI)

				if err != nil {
					return fmt.Errorf("failed to get list members from API for %s: %w", listConfig.ListURI, err)
				}

				for _, member := range listMembers.Items {
					user := persist.User{
						DID: member.Subject.Did,
					}

					if _, err := tx.NewInsert().Model(&user).
						Ignore().
						Exec(ctx, &user); err != nil && err != sql.ErrNoRows {
						return fmt.Errorf("upsertUser blind insert failed: %w", err)
					}

					if err := tx.NewSelect().Model(&user).Where("did = ?", user.DID).Scan(ctx); err != nil {
						return fmt.Errorf("upsertUser select failed: %w", err)
					}

					allApiMembers[member.Uri] = &persist.ListToUser{
						UserID: user.ID,
						User:   &user,
						ListID: list.ID,
						List:   &list,
						URI:    member.Uri,
					}
				}

				if listMembers.Cursor == nil || *listMembers.Cursor == "" {
					break
				}
				cursor = *listMembers.Cursor
			}

		}
		var dbURIs []string
		if err := tx.NewSelect().Model((*persist.ListToUser)(nil)).
			Column("uri").
			Scan(ctx, &dbURIs); err != nil && err != sql.ErrNoRows {
			return err
		}

		dbURIsMap := make(map[string]struct{})
		for _, uri := range dbURIs {
			dbURIsMap[uri] = struct{}{}
		}

		var membersToAdd []*persist.ListToUser
		for uri, listToUser := range allApiMembers {
			if _, found := dbURIsMap[uri]; !found {
				membersToAdd = append(membersToAdd, listToUser)
			}
		}

		var membersToDelete []string
		for uri, _ := range dbURIsMap {
			if _, found := allApiMembers[uri]; !found {
				membersToDelete = append(membersToDelete, uri)
			}
		}

		if len(membersToAdd) > 0 {
			if _, err := tx.NewInsert().Model(&membersToAdd).Exec(ctx); err != nil {
				return fmt.Errorf("adding users to lists failed: %w", err)
			}
		}

		if len(membersToDelete) > 0 {
			if _, err := tx.NewDelete().Model((*persist.ListToUser)(nil)).
				Where("uri IN (?)", membersToDelete).
				Exec(ctx); err != nil {
				return fmt.Errorf("removing users from lists failed: %w", err)
			}
		}

		return nil
	})

	var result []string
	for _, member := range allApiMembers {
		if !slices.Contains(result, member.User.DID) {
			result = append(result, member.User.DID)
		}
	}

	if len(result) == 0 {
		result = nil
	}

	return result, err
}

func initSubState(ctx context.Context, cfg *config.ServiceConfig, db *bun.DB) (string, error) {
	var serviceName string
	if serviceName = cfg.ServiceDID; cfg.ServiceDID == "" {
		serviceName = fmt.Sprintf("did:web:%s", cfg.Host)
	}

	subState := persist.SubscriptionState{
		Service: serviceName,
	}

	if _, err := db.NewInsert().Model(&subState).Ignore().Exec(ctx); err != nil {
		return "", fmt.Errorf("unable to set subscription state: %w", err)
	}

	return serviceName, nil
}
