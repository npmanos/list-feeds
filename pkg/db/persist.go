package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
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
	cursorUpdate := time.NewTicker(5 * time.Second)
	defer cursorUpdate.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping post op persister...")
			return
		case event := <-events:
			select{
			case <- cursorUpdate.C:
				if fn := writeCursor(serviceName, event.Cursor); fn != nil {
					dbTxs <- fn
				}
			default:
			}

			switch event.Kind {
			case jetstream.AccountEvent, jetstream.IdentityEvent:
				continue
			case jetstream.CommitEvent:
				commit := event.Commit

				if commit.Operation == jetstream.CommitDelete {
					if fn := deleteRecord(event); fn != nil {
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
			Author:    author,
			CreatedAt: record.CreatedAt,
		}

		if record.Reply != nil {
			parent, err := upsertThreadPost(ctx, record.Reply.Parent.URI, tx)
			if err != nil {
				return err
			}

			root, err := upsertThreadPost(ctx, record.Reply.Root.URI, tx)
			if err != nil {
				return err
			}

			post.ReplyParent = parent
			post.ReplyRoot = root
		}

		_, err = tx.NewInsert().Model(&post).Exec(ctx)

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
		}

		repost := Repost{
			ReposterID: reposter.ID,
			Reposter:   reposter,
			PostID:     post.ID,
			Post:       post,
			CreatedAt:  record.CreatedAt,
			URI:        utils.BuildAtURI(event.DID, event.Commit.Collection, event.Commit.RKey),
		}

		_, err = tx.NewInsert().Model(&repost).Exec(ctx)

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
		}

		like := Like{
			LikerID:   liker.ID,
			Liker:     liker,
			PostID:    post.ID,
			Post:      post,
			CreatedAt: record.CreatedAt,
			URI:       utils.BuildAtURI(event.DID, event.Commit.Collection, event.Commit.RKey),
		}

		_, err = tx.NewInsert().Model(&like).Exec(ctx)

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
		return nil, fmt.Errorf("unable to fetch post from API: %s", atURI)
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
		Author:    author,
		CreatedAt: postRecord.CreatedAt,
	}

	if postRecord.Reply != nil {
		parentPost, err := upsertThreadPost(ctx, postRecord.Reply.Parent.URI, tx)
		if err != nil {
			return nil, err
		}

		post.ReplyParent = parentPost

		rootPost, err := upsertThreadPost(ctx, postRecord.Reply.Root.URI, tx)
		if err != nil {
			return nil, err
		}

		post.ReplyRoot = rootPost
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

func deleteRecord(event *jetstream.Event) TxFn {
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
