package server

import (
	"net/http"
	"time"

	"github.com/npmanos/list-feeds/pkg/config"
	"github.com/uptrace/bun"
)

func addRoutes(
	mux *http.ServeMux,
	cfg *config.Config,
	db *bun.DB,
) {
	mux.Handle("/xrpc/app.bsky.feed.describeFeedGenerator", handleDescribeFeedGen(*cfg))
	mux.Handle("/xrpc/app.bsky.feed.getFeedSkeleton", handleGetFeedSkeleton(*cfg, db))
	mux.Handle("/_health", handleHealth(
		cfg.ServiceConfig.ServiceDID,
		time.Duration(cfg.ServiceConfig.MaxLagSecs) * time.Second,
		db,
	))
	mux.Handle("/", handleDefault())
}