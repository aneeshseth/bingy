package api

import (
	bloomfilter "binge/bloom_filter"
	"binge/cache"
	"binge/db"
	"binge/es"
	"context"
	"net/http"

	"github.com/go-chi/chi"
)

type API struct {
	httpC *http.Client
	db    *db.DB
	ctx   context.Context
	cache *cache.Cache
	es    *es.ES
	bfpu  *bloomfilter.BloomFilterPerUser
}

func NewAPIServer(database *db.DB, cache *cache.Cache, es *es.ES, bf *bloomfilter.BloomFilterPerUser) *chi.Mux {
	api := &API{
		httpC: &http.Client{},
		db:    database,
		ctx:   context.Background(),
		cache: cache,
		es:    es,
		bfpu:  bf,
	}

	r := chi.NewRouter()

	r.Route("/users", func(r chi.Router) {
		r.Post("/", api.createUser)
		r.Get("/feed", api.fetchFeed)
	})
	r.Route("/matches", func(r chi.Router) {
		r.Post("/", api.createMatch)
	})
	r.Route("/swipes", func(r chi.Router) {
		r.Post("/", api.createSwipe)
		r.Post("/atomic", api.atomicSwipe)
	})

	return r
}
