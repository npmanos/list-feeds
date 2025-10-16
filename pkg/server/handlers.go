package server

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/npmanos/list-feeds/pkg/config"
	persist "github.com/npmanos/list-feeds/pkg/db"
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

			for _, feedCfg := range cfg.ListFeedConfigs {
				if feedCfg.ChronologicalConfig.Enabled {
					desc.Feeds = append(
						desc.Feeds,
						feedgen.FeedURI{
							URI: utils.BuildAtURI(feedCfg.FeedDID, "app.bsky.feed.generator", feedCfg.ChronologicalConfig.Slug),
						},
					)
				}

				if feedCfg.PopularConfig.Enabled {
					desc.Feeds = append(
						desc.Feeds,
						feedgen.FeedURI{
							URI: utils.BuildAtURI(feedCfg.FeedDID, "app.bsky.feed.generator", feedCfg.PopularConfig.Slug),
						},
					)
				}
			}
		})

		utils.HttpEncode(w, r, http.StatusOK, desc)
	})
}

func handleGetFeedSkeleton(cfg config.Config, db *bun.DB) http.Handler {
	var init sync.Once
	feedMap := make(map[string]feedgen.FeedBuilder, 0)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		init.Do(func() {
			for _, feedCfg := range cfg.ListFeedConfigs {
				if feedCfg.ChronologicalConfig.Enabled {
					uri := utils.BuildAtURI(feedCfg.FeedDID, "app.bsky.feed.generator", feedCfg.ChronologicalConfig.Slug)
					feedMap[uri] = feedgen.NewChronologicalFeed(
						feedCfg.ListURI,
						cfg.ServiceConfig.ServiceDID,
						time.Duration(cfg.ServiceConfig.MaxLagSecs) * time.Second,
						feedCfg.ChronologicalConfig,
						db,
					)
				}

				if feedCfg.PopularConfig.Enabled {
					uri := utils.BuildAtURI(feedCfg.FeedDID, "app.bsky.feed.generator", feedCfg.PopularConfig.Slug)
					feedMap[uri] = feedgen.NewPopularFeed(
						feedCfg.ListURI,
						cfg.ServiceConfig.ServiceDID,
						time.Duration(cfg.ServiceConfig.MaxLagSecs) * time.Second,
						feedCfg.PopularConfig,
						db,
					)
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

func handleHealth(serviceName string, maxLag time.Duration, db *bun.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status := struct {
			Status string `json:"status"`
			Lag float64 `json:"lag,omitzero"`
		}{}

		lag, err := persist.GetLag(r.Context(), serviceName, db)
		if err != nil {
			status.Status = "unhealthy"
		} else if lag > maxLag {
			status.Status = "behind"
			status.Lag = lag.Seconds()
		} else {
			status.Status = "ok"
		}

		utils.HttpEncode(w, r, http.StatusOK, status)
	})
}

type didServiceDoc struct {
	ID              string `json:"id"`
	ServiceEndpoint string `json:"serviceEndpoint"`
	Type            string `json:"type"`
}

type didDoc struct {
	Context []string        `json:"@context"`
	ID      string          `json:"id"`
	Service []didServiceDoc `json:"service"`
}

func handleWellKnownDid(serviceDid string, host string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		didDoc := didDoc{
			Context: []string{"https://www.w3.org/ns/did/v1"},
			ID: serviceDid,
			Service: []didServiceDoc{
				didServiceDoc{
					ID: "#bsky_fg",
					ServiceEndpoint: fmt.Sprintf("https://%s", host),
					Type: "BskyFeedGenerator",
				},
			},
		}

		utils.HttpEncode(w, r, http.StatusOK, didDoc)
	})
}

func handleDefault() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		utils.HttpEncode(w, r, http.StatusNotFound, errResponse{notFound})
	})
}