package server

import (
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/npmanos/list-feeds/pkg/config"
	"github.com/npmanos/list-feeds/pkg/feedgen"
	"github.com/npmanos/list-feeds/pkg/utils"
	"github.com/uptrace/bun"
)

type statusErr string

const (
	notFound statusErr = "Not Found"
	badRequest statusErr = "Bad Request"
	internalError statusErr = "Internal Server Error"
)

type errResponse struct {
	Message statusErr `json:"message"`
}

func handleDescribeFeedGen(cfg config.Config) http.Handler {
	var (
		init sync.Once
		desc feedgen.FeedGenDescription
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		init.Do(func() {
			var feeds []feedgen.FeedURI
			desc = feedgen.FeedGenDescription{DID: cfg.ServiceConfig.ServiceDID, Feeds: feeds}

			for _, feed := range cfg.ListFeedConfigs {
				desc.Feeds = append(desc.Feeds, feedgen.FeedURI{URI: feed.ListURI})
			}
		})

		utils.HttpEncode(w, r, http.StatusOK, desc)
	})
}

func handleGetFeedSkeleton(cfg []config.ListFeedConfig, db *bun.DB) http.Handler {
	var init sync.Once
	feedMap := make(map[string]feedgen.FeedBuilder, 0)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		init.Do(func() {
			for _, feedCfg := range cfg {
				if feedCfg.ChronologicalConfig.Enabled {
					uri := utils.BuildAtURI(feedCfg.FeedDID, "app.bsky.feed.generator", feedCfg.ChronologicalConfig.Slug)
					feedMap[uri] = feedgen.NewChronologicalFeed(
						feedCfg.ListURI,
						feedCfg.ChronologicalConfig,
						db,
					)
				}

				if feedCfg.PopularConfig.Enabled {
					// uri := utils.BuildAtURI(feedCfg.FeedDID, "app.bsky.feed.generator", feedCfg.PopularConfig.Slug)
					// TODO
				}
			}
		})

		feedURI := r.URL.Query().Get("feed")
		builder, ok := feedMap[feedURI]
		if !ok {
			utils.HttpEncode(w, r, http.StatusBadRequest, errResponse{badRequest})
			return 
		}


		cursor := r.URL.Query().Get("cursor")
		limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
		if err != nil {
			limit = 20
		}

		skeleton, err := builder.BuildFeed(r.Context(), cursor, limit)
		if err != nil {
			feedURI := strings.Split(r.URL.Query().Get("feed"), "/")
			feed := feedURI[len(feedURI)-1]
			log.Printf("failed to build feed %s: %v", feed, err)
			utils.HttpEncode(w, r, http.StatusInternalServerError, errResponse{internalError})
			return
		}

		utils.HttpEncode(w, r, http.StatusOK, skeleton)
	})
}

func handleHealth(db *bun.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status := struct {
			status string `json:"status"`
			lag int64 `json:"lag"`
		} {
			status: "ok",
		}

		utils.HttpEncode(w, r, http.StatusOK, status)
	})
}

func handleDefault() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		utils.HttpEncode(w, r, http.StatusNotFound, errResponse{notFound})
	})
}