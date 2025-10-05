package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/npmanos/list-feeds/pkg/atpclient"
	"github.com/npmanos/list-feeds/pkg/jetstream"
	"github.com/uptrace/bun"

	"github.com/bluesky-social/indigo/api/agnostic"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
)

func PersistPostsOps(ctx context.Context, events <-chan *jetstream.Event, dbTxs chan<- TxFn, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-events:
			switch event.Kind {
			case jetstream.AccountEvent, jetstream.IdentityEvent:
				continue
			case jetstream.CommitEvent:
				commit := event.Commit
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

func WriteDb(ctx context.Context, db *bun.DB, dbTxs <-chan TxFn, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <- ctx.Done():
			return
		case txFn := <- dbTxs:
			if err := db.RunInTx(ctx, nil, txFn); err != nil {
				log.Printf("failed to execute transaction: %v", err)
			}
		}
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
			URI: fmt.Sprintf("at://%s/%s/%s", event.DID, record.Type, event.Commit.RKey),
			CID: event.Commit.CID,
			Author: author,
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

		repost := Repost {
			Reposter: reposter,
			Post: post,
			CreatedAt: record.CreatedAt,
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

		like := Like {
			Liker: liker,
			Post: post,
			CreatedAt: record.CreatedAt,
		}

		_, err = tx.NewInsert().Model(&like).Exec(ctx)

		return err
	}

	return fn, nil
}

func extractDID(atURI string) (string, error) {
	did, _ := strings.CutPrefix(atURI, "at://")
	did = strings.Split(did, "/")[0]

	if !strings.HasPrefix(did, "did:") {
		return "", fmt.Errorf("couldn't find a valid DID in list URI %s", atURI)
	}

	return did, nil
}

func upsertUser(ctx context.Context, did string, tx bun.Tx) (*User, error) {
	user := User{
		DID: did,
	}

	if _, err := tx.NewInsert().Model(&user).
		Ignore().
		Returning("*").
		Exec(ctx, &user); err != nil {
		return nil, err
	}

	return &user, nil
}

func upsertThreadPost(ctx context.Context, atURI string, tx bun.Tx) (*Post, error) {
	var post Post
	err := tx.NewSelect().Model(&post).
		Where("uri = ?", atURI).
		Scan(ctx);
	
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

	postRkey := strings.TrimPrefix(apiPost.Uri, fmt.Sprintf("at://%s/app.bsky.feed.post/", author.DID))

	postRecordOutput, err := agnostic.RepoGetRecord(ctx, atpClient, apiPost.Cid, "app.bsky.feed.post", author.DID, postRkey)
	if err != nil {
		return nil, err
	}

	var postRecord jetstream.PostRecord
	err = json.Unmarshal(*postRecordOutput.Value, &postRecord)
	if err != nil {
		return nil, err
	}

	post = Post {
		URI: apiPost.Uri,
		CID: apiPost.Cid,
		Author: author,
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

	if _, err := tx.NewInsert().Model(&post).
		Ignore().
		Returning("*").
		Exec(ctx, &post); err != nil {
			return nil, err
		}
	
	return &post, nil
}
