package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/npmanos/list-feeds/pkg/atpclient"
	"github.com/npmanos/list-feeds/pkg/config"
	persist "github.com/npmanos/list-feeds/pkg/db"
	"github.com/npmanos/list-feeds/pkg/db/migrations"
	"github.com/npmanos/list-feeds/pkg/jetstream"
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

	if err := syncLists(ctx, cfg.ListConfigs, db); err != nil {
		log.Fatalf("list sync failed: %v", err)
	}

	memberDids, err := refreshLists(ctx, cfg.ListConfigs, db)
	if err != nil {
		log.Fatalf("list member sync failed: %v", err)
	}

	// listOwnerDids, err := utils.Map(cfg.ListConfigs, func(lc config.ListConfig) (string, error) { return lc.DID() })
	// if err != nil {
	// 	log.Fatalln(err)
	// }

	log.Println("Application ready")

	wg := new(sync.WaitGroup)

	dbTxs := make(chan persist.TxFn)
	wg.Add(1)
	go persist.StartDbWriter(ctx, db, dbTxs, wg)
	wg.Add(1)
	go persist.StartDbJanitor(ctx, cfg.ServiceConfig.MaxAgeDays, dbTxs, db, wg)

	jetstreamEvents := make(chan *jetstream.Event)
	// for i := 0; i < 4; i++ {
	// 	wg.Add(1)
	// 	go persist.StartPostOpPersister(ctx, serviceName, jetstreamEvents, dbTxs, wg)
	// }
	wg.Add(1)
	go persist.StartPostOpPersister(ctx, serviceName, jetstreamEvents, dbTxs, wg)

	subState := persist.SubscriptionState{Service: serviceName}
	cursor, err := subState.GetCursor(ctx, db)
	if err != nil {
		log.Fatalf("failed to load cursor from db: %w", err)
	}

	postConsumer := jetstream.NewJetstreamConsumer(&jetstream.JetstreamConfig{
		Name:              "Post consumer",
		Hosts:             cfg.JetstreamHosts,
		Cursor:            cursor,
		WantedDids:        memberDids,
		WantedCollections: jetstream.POST_COLLECTIONS,
		MaxSize:           0,
		ExtraHeaders:      http.Header{},
		EventsChannel:     jetstreamEvents,
	})

	// listChangeConsumer := jetstream.NewJetstreamConsumer(&jetstream.JetstreamConfig{
	// 	Name:              "List change consumer",
	// 	Hosts:             cfg.JetstreamHosts,
	// 	Cursor:            1,
	// 	WantedDids:        listOwnerDids,
	// 	WantedCollections: jetstream.LIST_MEMBER_COLLECTIONS,
	// 	MaxSize:           0,
	// 	ExtraHeaders:      http.Header{},
	// })

	log.Println("Running... Press Ctrl+C to exit.")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	wg.Add(1)
	go postConsumer.Start(ctx, wg)
	// wg.Add(1)
	// go listChangeConsumer.Start(ctx, wg)

	<-quit
	log.Println("Shutting down...")
	cancel()

	wg.Wait()
}

func syncLists(ctx context.Context, listConfigs []config.ListConfig, db *bun.DB) error {
	log.Println("Syncing lists with config file...")

	// 1. Get all list URIs from the config file into a map for easy lookup.
	configListURIs := make(map[string]struct{})
	for _, lc := range listConfigs {
		configListURIs[lc.URI] = struct{}{}
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

func refreshLists(ctx context.Context, listConfigs []config.ListConfig, db *bun.DB) ([]string, error) {
	apiClient := atpclient.GetATProtoClient()
	allMemberDids := make(map[string]struct{})

	for _, listConfig := range listConfigs {
		log.Printf("Syncing members for list %s", listConfig.URI)

		apiMembers := make(map[string]*appbsky.GraphDefs_ListItemView)
		var cursor string
		for {
			listMembers, err := appbsky.GraphGetList(ctx, apiClient, cursor, 100, listConfig.URI)

			if err != nil {
				return nil, fmt.Errorf("failed to get list members from API for %s: %w", listConfig.URI, err)
			}

			for _, member := range listMembers.Items {
				apiMembers[member.Subject.Did] = member
				allMemberDids[member.Subject.Did] = struct{}{}
			}

			if listMembers.Cursor == nil || *listMembers.Cursor == "" {
				break
			}
			cursor = *listMembers.Cursor
		}

		err := db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			var list persist.List

			err := tx.NewSelect().
				Model(&list).
				Where("uri = ?", listConfig.URI).
				Relation("ListMembers").
				Scan(ctx)

			if err != nil && err != sql.ErrNoRows {
				return err
			}

			list.URI = listConfig.URI

			dbMembers := make(map[string]struct{})
			for _, member := range list.ListMembers {
				dbMembers[member.DID] = struct{}{}
			}

			var memberDidsToAdd []string
			var memberDidsToRemove []string

			for did := range apiMembers {
				if _, found := dbMembers[did]; !found {
					memberDidsToAdd = append(memberDidsToAdd, did)
				}
			}

			for did := range dbMembers {
				if _, found := apiMembers[did]; !found {
					memberDidsToRemove = append(memberDidsToRemove, did)
				}
			}

			if len(memberDidsToAdd) > 0 {
				log.Printf("Adding %d members to list %s", len(memberDidsToAdd), listConfig.URI)

				var usersToInsert []persist.User
				for _, did := range memberDidsToAdd {
					usersToInsert = append(usersToInsert, persist.User{DID: did})
				}

				_, err := tx.NewInsert().Model(&usersToInsert).
					On("CONFLICT (did) DO NOTHING").
					Exec(ctx)
				if err != nil {
					return fmt.Errorf("failed to bulk insert users: %w", err)
				}

				var usersForJoin []persist.User
				if err := tx.NewSelect().Model(&usersForJoin).Where("did IN (?)", bun.In(memberDidsToAdd)).Scan(ctx); err != nil {
					return fmt.Errorf("failed to select users for join: %w", err)
				}

				var membersToAdd []persist.ListToUser
				for i := range usersForJoin {
					membersToAdd = append(membersToAdd, persist.ListToUser{
						ListID: list.ID,
						UserID: usersForJoin[i].ID,
					})
				}

				_, err = tx.NewInsert().Model(&membersToAdd).Exec(ctx)
				if err != nil {
					return fmt.Errorf("failed to insert list members: %w", err)
				}
			}

			if len(memberDidsToRemove) > 0 {
				log.Printf("Removing %d members from list %s", len(memberDidsToRemove), listConfig.URI)
				var usersToRemove []persist.User
				if err := tx.NewSelect().Model(&usersToRemove).Where("did IN (?)", bun.In(memberDidsToRemove)).Scan(ctx); err != nil {
					return err
				}

				_, err := tx.NewDelete().Model((*persist.ListToUser)(nil)).
					Where("list_id = ? AND user_id IN (?)", list.ID, bun.In(usersToRemove)).
					Exec(ctx)
				if err != nil {
					return err
				}
			}

			return nil
		})

		if err != nil {
			return nil, err
		}
	}

	result := make([]string, 0, len(allMemberDids))
	for did := range allMemberDids {
		result = append(result, did)
	}

	return result, nil
}

func initSubState(ctx context.Context, cfg config.ServiceConfig, db *bun.DB) (string, error) {
	var serviceName string
	if serviceName = cfg.ServiceDID; cfg.ServiceDID == "" {
		serviceName = fmt.Sprintf("did:web:%s", cfg.Host)
	}

	subState := persist.SubscriptionState {
		Service: serviceName,
	}
	
	if _, err := db.NewInsert().Model(&subState).Ignore().Exec(ctx); err != nil {
		return "", fmt.Errorf("unable to set subscription state: %w", err)
	}

	return serviceName, nil
}