package server

import (
	"net/http"

	"github.com/npmanos/list-feeds/pkg/config"
	"github.com/uptrace/bun"
)

func NewServer(cfg *config.Config, db *bun.DB) http.Handler {
	mux := http.NewServeMux()
	addRoutes(mux, cfg, db)

	return mux
}