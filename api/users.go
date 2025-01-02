package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	bloomfilter "binge/bloom_filter"
	"binge/db/migr"
	"binge/es"

	"github.com/go-redis/redis"
)

type URequestBody struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Bio       string `json:"bio"`
	Longitude string `json:"longitude"`
	Latitude  string `json:"latitude"`
}

func (a *API) createUser(w http.ResponseWriter, r *http.Request) {
	var requestBody URequestBody
	err := json.NewDecoder(r.Body).Decode(&requestBody)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = a.db.InsertUser(a.ctx, migr.InsertUserParams{
		FirstName: requestBody.FirstName,
		LastName:  requestBody.LastName,
		Bio:       requestBody.Bio,
		Longitude: requestBody.Longitude,
		Latitude:  requestBody.Latitude,
	})

	if err != nil {
		log.Printf("error creating user: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	//init bloom filter for this user
	bf, err := bloomfilter.NewBloomFilterForUser(1024, fmt.Sprintf("%s-%s", requestBody.FirstName, requestBody.LastName))
	if err != nil {
		log.Printf("error creating user: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	a.bfpu = bf

	w.WriteHeader(http.StatusOK)
}

type FRequestBody struct {
	FirstName       string `json:"first_name"`
	LastName        string `json:"last_name"`
	Longitude       string `json:"longitude"`
	Latitude        string `json:"latitude"`
	DesiredDistance string `json:"distance"`
}

func (a *API) fetchFeed(w http.ResponseWriter, r *http.Request) {
	var requestBody FRequestBody
	err := json.NewDecoder(r.Body).Decode(&requestBody)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	feedKey := fmt.Sprintf("%s-%s:feed", requestBody.FirstName, requestBody.LastName)

	val, err := a.cache.R.Get(feedKey).Result()
	if err != nil {
		if err == redis.Nil {
			hits, err := a.es.RetrieveUserFilteredData("users",
				requestBody.Latitude,
				requestBody.Longitude,
				requestBody.DesiredDistance)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			var filteredResults []es.User
			for _, hit := range hits {
				isMember, err := a.bfpu.MembershipCheck(fmt.Sprintf("%s-%s", hit.Source.FirstName, hit.Source.LastName), fmt.Sprintf("%s-%s", requestBody.FirstName, requestBody.LastName))
				if err != nil {
					http.Error(w, "Error with membership checks in bloom filter", http.StatusInternalServerError)
					return
				}
				if !isMember && (fmt.Sprintf("%s-%s", hit.Source.FirstName, hit.Source.LastName) != fmt.Sprintf("%s-%s", requestBody.FirstName, requestBody.LastName)) {
					filteredResults = append(filteredResults, hit.Source)
				}
			}

			// Split results: 60% for cache, 40% for immediate return
			splitIndex := int(float64(len(filteredResults)) * 0.6)
			cacheResults := filteredResults[:splitIndex]
			immediateResults := filteredResults[splitIndex:]

			// Cache 60% of the results
			cacheData, err := json.Marshal(cacheResults)
			if err != nil {
				http.Error(w, "Error marshaling data for cache", http.StatusInternalServerError)
				return
			}

			err = a.cache.R.Set(feedKey, cacheData, time.Hour).Err()
			if err != nil {
				log.Printf("error setting cache: %v", err)
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			response, err := json.Marshal(immediateResults)
			if err != nil {
				http.Error(w, "Error processing response", http.StatusInternalServerError)
				return
			}
			w.Write(response)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Cache hit - parse and return cached data (60% of original results)
	var retrievedData []map[string]interface{}
	err = json.Unmarshal([]byte(val), &retrievedData)
	if err != nil {
		log.Printf("Error deserializing cached data: %v", err)
		http.Error(w, "Error processing cached data", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response, err := json.Marshal(retrievedData)
	if err != nil {
		http.Error(w, "Error processing response", http.StatusInternalServerError)
		return
	}
	_, err = a.cache.R.Del(feedKey).Result()
	if err != nil {
		log.Printf("error deleting cache: %v", err)
	}
	w.Write(response)
}
