package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"slices"
	"time"

	"github.com/bluesky-social/indigo/atproto/atclient"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/npmanos/list-feeds/pkg/config"
	"github.com/urfave/cli/v3"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
)

func main() {
	var configFile string
	var feeds []string
	var password string

	cmd := cli.Command{
		Name: "publish-feed",
		Usage: "list feed publisher",
		Version: "2025.1.0",
		Flags: []cli.Flag{
			&cli.StringSliceFlag {
				Name: "feed",
				Aliases: []string{"l"},
				Usage: "`LIST_URI` from config.yml for feed to publish",
				Destination: &feeds,
			},
			&cli.StringFlag {
				Name: "password",
				Aliases: []string{"p"},
				Usage: "`PASSWORD` (or app password) of the account to publish the feed to",
				Required: true,
				Destination: &password,
			},
			&cli.StringFlag {
				Name: "config",
				Aliases: []string{"c"},
				Value: "./data/config.yml",
				Usage: "path to config `FILE`",
				Destination: &configFile,
			},
		},
	}

	cmd.Action = func(ctx context.Context, c *cli.Command) error {
		cfg, err := config.LoadConfig(configFile)
		if err != nil {
			log.Fatalf("unable to load config: %v")
		}

		feedCfgs := make([]config.ListFeedConfig, len(cfg.ListFeedConfigs))
		if len(feeds) > 0 {
			feedCfgs = slices.Collect(func(yield func(config.ListFeedConfig) bool) {
				for _, feedCfg := range cfg.ListFeedConfigs {
					if slices.Contains(feeds, feedCfg.ListURI) {
						if !yield(feedCfg) {
							return
						}
					}
				}
			})
		} else {
			feedCfgs = cfg.ListFeedConfigs
		}

		return publish(ctx, cfg.ServiceConfig.ServiceDID, feedCfgs, password)
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatalf("failed to publish feeds: %v", err)
	}
}

func publish(ctx context.Context, serviceDid string, feedCfgs []config.ListFeedConfig, password string) error {
	dir := identity.DefaultDirectory()
	for _, feedCfg := range feedCfgs {
		atid, err := syntax.ParseAtIdentifier(feedCfg.FeedDID)
		if err != nil {
			return err
		}

		client, err := atclient.LoginWithPassword(ctx, dir, *atid, password, "", nil)
		if err != nil {
			return fmt.Errorf("unable to login with password: %w", err)
		}

		if feedCfg.ChronologicalConfig.Enabled {
			if err := putRecord(ctx, client, feedCfg.ChronologicalConfig.BaseFeedConfig, serviceDid); err != nil {
				return err
			}
		}

		if feedCfg.PopularConfig.Enabled {
			if err := putRecord(ctx, client, feedCfg.PopularConfig.BaseFeedConfig, serviceDid); err != nil {
				return err
			}
		}

		
	}

	return nil
}

func putRecord(ctx context.Context, client *atclient.APIClient, feedCfg config.BaseFeedConfig, serviceDid string) error {
	var avatarBlob *util.LexBlob
	if feedCfg.Avatar != "" {
		avatar, err := os.Open(feedCfg.Avatar)
		if err != nil {
			return fmt.Errorf("failed to open avatar file: %w", err)
		}

		avatarUpload, err := comatproto.RepoUploadBlob(ctx, client, avatar)
		if err != nil {
			return fmt.Errorf("failed to upload avatar: %w", err)
		}

		avatarBlob = avatarUpload.Blob
	}

	feedGenRecord := appbsky.FeedGenerator{
		Did: serviceDid,
		Avatar: avatarBlob,
		Description: &feedCfg.Description,
		DisplayName: feedCfg.Name,
		CreatedAt: time.Now().Format(time.RFC3339),
	}

	repoRecord := comatproto.RepoPutRecord_Input{
		Repo: client.AccountDID.String(),
		Collection: "app.bsky.feed.generator",
		Rkey: feedCfg.Slug,
		Record: &util.LexiconTypeDecoder{Val: &feedGenRecord},
	}
	
	if _, err := comatproto.RepoPutRecord(ctx, client, &repoRecord); err != nil {
		return fmt.Errorf("failed to write feedgen record to repo: %w", err)
	}

	return nil
}