package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/npmanos/list-feeds/pkg/atpclient"
	"github.com/npmanos/list-feeds/pkg/jetstream"
	"github.com/npmanos/list-feeds/pkg/utils"
	"github.com/uptrace/bun"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
)

func StartDbWriter(ctx context.Context, db *bun.DB, dbTxs <-chan TxFn, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Starting db writer...")
	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping db writer...")
			return
		case txFn := <-dbTxs:
			if err := db.RunInTx(ctx, nil, txFn); err != nil {
				log.Printf("failed to execute transaction: %v", err)
			}
		}
	}
}

func StartPostOpPersister(ctx context.Context, serviceName string, events <-chan *jetstream.Event, dbTxs chan<- TxFn, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Starting post op persister...")
	var lastCursor int64 = 0
	cursorUpdate := time.NewTicker(5 * time.Second)
	defer cursorUpdate.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping post op persister...")
			return
		case event := <-events:
			select {
			case <-cursorUpdate.C:
				if event.Cursor > lastCursor {
					if fn := writeCursor(serviceName, event.Cursor); fn != nil {
						dbTxs <- fn
						lastCursor = event.Cursor
						log.Printf("Cursor: %d", lastCursor)
					}
				}

			default:
			}

			switch event.Kind {
			case jetstream.AccountEvent, jetstream.IdentityEvent:
				continue
			case jetstream.CommitEvent:
				commit := event.Commit

				if commit.Operation == jetstream.CommitDelete {
					if fn := deletePostOpRecord(event); fn != nil {
						dbTxs <- fn
					}

					continue
				}

				switch commit.Record.(type) {
				case jetstream.PostRecord:
					if fn, err := persistPost(event); err != nil {
						log.Printf("unable to save post: %v", err)
					} else {
						dbTxs <- fn
					}
				case jetstream.RepostRecord:
					if fn, err := persistRepost(event); err != nil {
						log.Printf("unable to save repost: %v", err)
					} else {
						dbTxs <- fn
					}
				case jetstream.LikeRecord:
					if fn, err := persistLike(event); err != nil {
						log.Printf("unable to save like: %v", err)
					} else {
						dbTxs <- fn
					}
				}
			}
		}
	}
}

func StartDbJanitor(ctx context.Context, maxAgeDays float32, dbTxs chan<- TxFn, db *bun.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	if maxAgeDays <= 0 {
		return
	}

	log.Println("Starting database janitor...")
	dbTxs <- pruneDb(maxAgeDays)

	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			dbTxs <- pruneDb(maxAgeDays)
		case <-ctx.Done():
			log.Println("Stopping database janitor...")
			return
		}
	}
}

func StartListMemberPersister(
	ctx context.Context,
	jetstreamHosts []string,
	listMemberEvents <-chan *jetstream.Event,
	postOpEvents chan<- *jetstream.Event,
	didUpdates chan<- *jetstream.ListMemberUpdate,
	dbTxs chan<- TxFn,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	log.Printf("Starting list member persister...")

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping list member persister...")
			return
		case event := <-listMemberEvents:
			switch event.Kind {
			case jetstream.AccountEvent, jetstream.IdentityEvent:
				continue
			case jetstream.CommitEvent:
				commit := event.Commit

				switch commit.Operation {
				case jetstream.CommitCreate:
					addedDid, err := addListMember(ctx, jetstreamHosts, event, postOpEvents, dbTxs)
					if err != nil {
						log.Printf("error adding new list member: %v", err)
						continue
					}
					didUpdates <- &jetstream.ListMemberUpdate{AddMember: addedDid}
				case jetstream.CommitDelete:
					tx, removedDid := deleteListMember(event)
					dbTxs <- tx
					didUpdates <- &jetstream.ListMemberUpdate{RemoveMember: removedDid}
				}
			}
		}
	}
}

func pruneDb(maxAgeDays float32) TxFn {
	return func(ctx context.Context, tx bun.Tx) error {
		log.Printf("Pruning records older than %.2f days...", maxAgeDays)

		maxAgeDuration := time.Duration(maxAgeDays*24) * time.Hour
		cutoffTime := time.Now().Add(-maxAgeDuration)

		_, err := tx.NewDelete().Model((*Like)(nil)).
			Where("created_at < ?", cutoffTime).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to prune old likes: %w", err)
		}

		_, err = tx.NewDelete().Model((*Repost)(nil)).
			Where("created_at < ?", cutoffTime).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to prune old reposts: %w", err)
		}

		likesExist := tx.NewSelect().Model((*Like)(nil)).Where("post_id = p.id")
		repostsExist := tx.NewSelect().Model((*Repost)(nil)).Where("post_id = p.id")
		parentsExist := tx.NewSelect().Model((*Post)(nil)).Where("reply_parent_id = p.id")
		rootsExist := tx.NewSelect().Model((*Post)(nil)).Where("reply_root_id = p.id")

		_, err = tx.NewDelete().
			// Alias the posts table as "p" so the subqueries can reference it.
			TableExpr("posts AS p").
			Where("p.created_at < ?", cutoffTime).
			Where("NOT EXISTS (?)", likesExist).
			Where("NOT EXISTS (?)", repostsExist).
			Where("NOT EXISTS (?)", parentsExist).
			Where("NOT EXISTS (?)", rootsExist).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to prune old posts: %w", err)
		}

		log.Println("Pruned old records")

		return nil
	}
}

func persistPost(event *jetstream.Event) (TxFn, error) {
	record, ok := event.Commit.Record.(jetstream.PostRecord)
	if !ok {
		return nil, errors.New("was not a post record")
	}

	fn := func(ctx context.Context, tx bun.Tx) error {
		author, err := upsertUser(ctx, event.DID, tx)
		if err != nil {
			return err
		}

		post := Post{
			URI:       utils.BuildAtURI(event.DID, event.Commit.Collection, event.Commit.RKey),
			CID:       event.Commit.CID,
			AuthorID:    author.ID,
			CreatedAt: record.CreatedAt,
		}

		if record.Reply != nil {
			parent, err := upsertThreadPost(ctx, record.Reply.Parent.URI, tx)
			if err != nil {
				return err
			}

			if parent != nil {
				post.ReplyParentID = parent.ID
			}

			root, err := upsertThreadPost(ctx, record.Reply.Root.URI, tx)
			if err != nil {
				return err
			}

			if root != nil {
				post.ReplyRootID = root.ID
			}
		}

		_, err = tx.NewInsert().Model(&post).Ignore().Exec(ctx)

		return err
	}

	return fn, nil
}

func persistRepost(event *jetstream.Event) (TxFn, error) {
	record, ok := event.Commit.Record.(jetstream.RepostRecord)
	if !ok {
		return nil, errors.New("was not a repost record")
	}

	fn := func(ctx context.Context, tx bun.Tx) error {
		reposter, err := upsertUser(ctx, event.DID, tx)
		if err != nil {
			return err
		}

		post, err := upsertThreadPost(ctx, record.Subject.URI, tx)
		if err != nil {
			return err
		} else if post == nil {
			return nil
		}

		repost := Repost{
			ReposterID: reposter.ID,
			Reposter:   reposter,
			PostID:     post.ID,
			Post:       post,
			CreatedAt:  record.CreatedAt,
			URI:        utils.BuildAtURI(event.DID, event.Commit.Collection, event.Commit.RKey),
		}

		_, err = tx.NewInsert().Model(&repost).Ignore().Exec(ctx)

		return err
	}

	return fn, nil
}

func persistLike(event *jetstream.Event) (TxFn, error) {
	record, ok := event.Commit.Record.(jetstream.LikeRecord)
	if !ok {
		return nil, errors.New("was not a like record")
	}

	fn := func(ctx context.Context, tx bun.Tx) error {
		liker, err := upsertUser(ctx, event.DID, tx)
		if err != nil {
			return err
		}

		post, err := upsertThreadPost(ctx, record.Subject.URI, tx)
		if err != nil {
			return err
		} else if post == nil {
			return nil
		}

		like := Like{
			LikerID:   liker.ID,
			Liker:     liker,
			PostID:    post.ID,
			Post:      post,
			CreatedAt: record.CreatedAt,
			URI:       utils.BuildAtURI(event.DID, event.Commit.Collection, event.Commit.RKey),
		}

		_, err = tx.NewInsert().Model(&like).Ignore().Exec(ctx)

		return err
	}

	return fn, nil
}

func upsertUser(ctx context.Context, did string, tx bun.Tx) (*User, error) {
	user := User{
		DID: did,
	}

	if _, err := tx.NewInsert().Model(&user).
		Ignore().
		Exec(ctx, &user); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("upsertUser blind insert failed: %w", err)
	}

	if err := tx.NewSelect().Model(&user).Where("did = ?", user.DID).Scan(ctx); err != nil {
		return nil, fmt.Errorf("upsertUser select failed: %w", err)
	}

	return &user, nil
}

func upsertThreadPost(ctx context.Context, atURI string, tx bun.Tx) (*Post, error) {
	var post Post
	err := tx.NewSelect().Model(&post).
		Where("uri = ?", atURI).
		Scan(ctx)

	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	if err == nil {
		return &post, nil
	}

	atpClient := atpclient.GetATProtoClient()
	apiPosts, err := appbsky.FeedGetPosts(ctx, atpClient, []string{atURI})
	if err != nil {
		return nil, err
	}

	if len(apiPosts.Posts) == 0 {
		// return nil, fmt.Errorf("unable to fetch post from API: %s", atURI)
		return nil, nil
	}

	apiPost := apiPosts.Posts[0]

	author, err := upsertUser(ctx, apiPost.Author.Did, tx)
	if err != nil {
		return nil, err
	}

	apiPostRecord, ok := apiPost.Record.Val.(*appbsky.FeedPost)
	if !ok {
		return nil, fmt.Errorf("failed to assert record as *appbsky.FeedPost")
	}

	apiCreatedAt, err := time.Parse(time.RFC3339, apiPostRecord.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to parse apiCreatedAt: %w", err)
	}

	postRecord := jetstream.PostRecord{
		Type:      apiPostRecord.LexiconTypeID,
		CreatedAt: apiCreatedAt,
	}

	if apiPostRecord.Reply != nil {
		postRecord.Reply = &jetstream.PostReply{
			Parent: &jetstream.PostRef{
				CID: apiPostRecord.Reply.Parent.Cid,
				URI: apiPostRecord.Reply.Parent.Uri,
			},
			Root: &jetstream.PostRef{
				CID: apiPostRecord.Reply.Root.Cid,
				URI: apiPostRecord.Reply.Root.Uri,
			},
		}
	}

	post = Post{
		URI:       apiPost.Uri,
		CID:       apiPost.Cid,
		AuthorID:  author.ID,
		CreatedAt: postRecord.CreatedAt,
	}

	if postRecord.Reply != nil {
		parentPost, err := upsertThreadPost(ctx, postRecord.Reply.Parent.URI, tx)
		if err != nil {
			return nil, err
		}

		if parentPost != nil {
			post.ReplyParentID = parentPost.ID
		}

		rootPost, err := upsertThreadPost(ctx, postRecord.Reply.Root.URI, tx)
		if err != nil {
			return nil, err
		}

		if rootPost != nil {
			post.ReplyRootID = rootPost.ID
		}
	}

	_, err = tx.NewInsert().Model(&post).Ignore().Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("upsertThreadPost blind insert failed: %w", err)
	}

	if err := tx.NewSelect().Model(&post).Where("uri = ?", post.URI).Scan(ctx); err != nil {
		return nil, fmt.Errorf("upsertThreadPost select failed: %w", err)
	}

	return &post, nil
}

func deletePostOpRecord(event *jetstream.Event) TxFn {
	commit := event.Commit
	uri := utils.BuildAtURI(event.DID, event.Commit.Collection, event.Commit.RKey)

	switch commit.Collection {
	case "app.bsky.feed.post":
		return makeDeleteFn(uri, (*Post)(nil))
	case "app.bsky.feed.repost":
		return makeDeleteFn(uri, (*Repost)(nil))
	case "app.bsky.feed.like":
		return makeDeleteFn(uri, (*Like)(nil))
	default:
		return nil
	}
}

func makeDeleteFn(uri string, model interface{}) TxFn {
	return func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewDelete().Model(model).Where("uri = ?", uri).Exec(ctx)
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
}

func writeCursor(serviceName string, cursor int64) TxFn {
	return func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewUpdate().Model((*SubscriptionState)(nil)).
			Column("cursor").
			Set("cursor = ?", cursor).
			Where("service = ?", serviceName).
			Exec(ctx)

		return err
	}
}

func addListMember(
	ctx context.Context,
	jetstreamHosts []string,
	event *jetstream.Event,
	postOpEvents chan<- *jetstream.Event,
	dbTxs chan<- TxFn,
) (string, error) {
	record, ok := event.Commit.Record.(jetstream.ListItemRecord)
	if !ok {
		return "", fmt.Errorf("was not a list item record: %v", record)
	}

	dbTxs <- func(ctx context.Context, tx bun.Tx) error {
		var list List
		if err := tx.NewSelect().Model(&list).Where("uri = ?", record.List).Scan(ctx); err == sql.ErrNoRows {
			return nil
		} else if err != nil {
			return err
		}

		member, err := upsertUser(ctx, record.Subject, tx)
		if err != nil {
			return err
		}

		listMembership := ListToUser{
			List: &list,
			User: member,
			URI:  event.Commit.RKey,
		}
		if _, err := tx.NewInsert().Model(&listMembership).Ignore().Exec(ctx); err != nil {
			return fmt.Errorf("unable to add %s to list %s: %w", record.List, record.Subject, err)
		}

		return nil
	}

	backfillEvents := make(chan *jetstream.Event)
	backfillCtx, cancelBackfill := context.WithCancel(ctx)
	backfillWg := new(sync.WaitGroup)
	backfillConfig := jetstream.JetstreamConfig{
		Name:              fmt.Sprintf("%s backill consumer", record.Subject),
		Hosts:             jetstreamHosts,
		Cursor:            1,
		WantedDids:        []string{record.Subject},
		WantedCollections: jetstream.POST_COLLECTIONS,
		MaxSize:           0,
		ExtraHeaders:      http.Header{},
		EventsChannel:     backfillEvents,
	}

	backfillConsumer := jetstream.NewJetstreamConsumer(&backfillConfig)

	backfillWg.Add(1)
	go backfillConsumer.Start(backfillCtx, backfillWg)

	for {
		select {
		case <-ctx.Done():
			cancelBackfill()
			backfillWg.Wait()
			return record.Subject, nil
		case backfillEvent := <-backfillEvents:
			if backfillEvent.Cursor >= event.Cursor {
				cancelBackfill()
				backfillWg.Wait()
				return record.Subject, nil
			}
			postOpEvents <- backfillEvent
		}
	}
}

func deleteListMember(event *jetstream.Event) (TxFn, string) {
	var removedDid string
	return func(ctx context.Context, tx bun.Tx) error {
		uri := utils.BuildAtURI(event.DID, event.Commit.Collection, event.Commit.RKey)
		var removedListToUser *ListToUser
		err := tx.NewSelect().Model(&removedListToUser).
			Relation("User").
			Relation("List").
			Where("uri = ?", uri).
			Scan(ctx)

		if err != nil {
			return fmt.Errorf("error deleting list member %s, %w", uri, err)
		}

		removedUser := removedListToUser.User
		listCount, err := tx.NewSelect().Model((*ListToUser)(nil)).Where("user_id = ?", removedUser.ID).Count(ctx)

		if err != nil {
			return fmt.Errorf("error deleting list member %s, %w", uri, err)
		}

		if listCount == 1 {
			removedDid = removedUser.DID
		}

		if _, err = tx.NewDelete().Model(removedListToUser).
			Where("uri = ?", uri).
			Exec(ctx); err != nil {
			return fmt.Errorf("error deleting list member %s, %w", uri, err)
		}

		return nil
	}, removedDid
}
