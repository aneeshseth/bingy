package api

import (
	"binge/db/migr"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type SRequestBody struct {
	UserId1        int64  `json:"user_id_1"`
	UserId2        int64  `json:"user_id_2"`
	SwipeDirection string `json:"swipe_direction"`
}

func (a *API) atomicSwipe(w http.ResponseWriter, r *http.Request) {
	var requestBody SRequestBody
	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	key := getKey(requestBody.UserId1, requestBody.UserId2)

	luaScript := `
	redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
	return redis.call('HGET', KEYS[1], ARGV[3])
	`

	swipeField := getSwipeField(requestBody.UserId1)
	otherField := getSwipeField(requestBody.UserId2)
	result, err := a.cache.R.Eval(luaScript, []string{key}, swipeField, requestBody.SwipeDirection, otherField).Result()
	if err != nil {
		log.Printf("error executing redis lua script: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if requestBody.SwipeDirection == "right" && result == "right" {
		err := a.createMatchHandler(requestBody.UserId1, requestBody.UserId2)
		if err != nil {
			log.Printf("error creating match: %v", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func getKey(userA, userB int64) string {
	if userA > userB {
		userA, userB = userB, userA
	}
	return "swipes:" + fmt.Sprintf("%d:%d", userA, userB)
}

func getSwipeField(userA int64) string {
	return fmt.Sprintf("%d_swipe", userA)
}

func (a *API) createMatchHandler(userA, userB int64) error {
	return nil
}

func (a *API) createSwipe(w http.ResponseWriter, r *http.Request) {
	var requestBody []SRequestBody
	err := json.NewDecoder(r.Body).Decode(&requestBody)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, swipe := range requestBody {
		swipeType := migr.SwipesSwipeType(swipe.SwipeDirection)
		err := a.db.InsertSwipe(a.ctx, migr.InsertSwipeParams{
			UserSwiped:   swipe.UserId1,
			UserSwipedOn: swipe.UserId2,
			SwipeType:    swipeType,
		})
		if err != nil {
			log.Printf("error inserting swipe: %v", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}
