package main

import (
	"binge/api"
	bloomfilter "binge/bloom_filter"
	"binge/cache"
	"binge/db"
	"binge/es"
	"log"
	"net/http"
	"os"

	"github.com/go-chi/chi"
)

type Binge interface {
	DBService() error
	ESService() error
	CacheService()
	BloomFilter() error
	APIService() *chi.Mux
}

func RunApp(binge Binge) *chi.Mux {
	err := binge.DBService()

	if err != nil {
		log.Fatalf("Error setting up DB service: %v", err)
	}

	binge.CacheService()

	binge.BloomFilter()

	binge.ESService()

	return binge.APIService()
}

type BingeService struct {
	db    *db.DB
	cache *cache.Cache
	es    *es.ES
	bf    *bloomfilter.BloomFilterPerUser
}

func (b *BingeService) DBService() error {
	db, err := db.NewDB(os.Getenv("SQL_USER"), os.Getenv("SQL_PASS"), os.Getenv("GLOBAL_DB"))
	if err != nil {
		log.Fatal(err)
		return err
	}
	b.db = db
	log.Println("DB connection initialized")
	return nil
}

func (b *BingeService) CacheService() {
	c := cache.NewCache("6379")
	log.Println("Cache initialized")
	b.cache = c
}

func (b *BingeService) BloomFilter() error {
	bf, err := bloomfilter.InitializeGlobalBloomFilter()
	if err != nil {
		return err
	}
	b.bf = bf
	log.Println("Bloom filter service started")
	return nil
}

func (b *BingeService) APIService() *chi.Mux {
	return api.NewAPIServer(b.db, b.cache, b.es, b.bf)
}

func (b *BingeService) ESService() error {
	client, bi, index, err := es.NewClient(os.Getenv("CLOUD_ID_ES"), "users", os.Getenv("API_KEY_ES"))
	if err != nil {
		return err
	}
	b.es = &es.ES{
		Cl:    client,
		Bi:    bi,
		Index: index,
	}
	log.Println("ES service started")
	return nil
}

func main() {
	bingeService := &BingeService{}
	server := RunApp(bingeService)
	log.Println("API server starting")
	http.ListenAndServe(":3000", server)
}
