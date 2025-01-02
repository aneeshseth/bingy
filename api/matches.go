package api

import (
	"binge/db/migr"
	"encoding/json"
	"log"
	"net/http"
)

type MRequestBody struct {
	UserId1 int64 `json:"user_id_1"`
	UserId2 int64 `json:"user_id_2"`
}

func (a *API) createMatch(w http.ResponseWriter, r *http.Request) {
	var requestBody MRequestBody
	err := json.NewDecoder(r.Body).Decode(&requestBody)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = a.db.InsertMatch(a.ctx, migr.InsertMatchParams{
		UserID1: requestBody.UserId1,
		UserID2: requestBody.UserId2,
	})
	if err != nil {
		log.Printf("error creating match: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
