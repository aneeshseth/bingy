package cdc

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/tidwall/gjson"
)

func TransformCreateOperationForES(record map[string]interface{}) ([]byte, error) {
	v, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	lat := gjson.GetBytes(v, "latitude")
	long := gjson.GetBytes(v, "longitude")

	decodedLatitude, err := base64.StdEncoding.DecodeString(lat.String())
	if err != nil {
		return nil, err
	}
	decodedLongitude, err := base64.StdEncoding.DecodeString(long.String())
	if err != nil {
		return nil, err
	}
	fmt.Println(decodedLatitude)
	fmt.Println(decodedLongitude)

	rand.Seed(time.Now().UnixNano())

	latitude := rand.Float64()*180 - 90
	longitude := rand.Float64()*360 - 180

	esData := map[string]interface{}{
		"id":         record["id"],
		"first_name": record["first_name"],
		"last_name":  record["last_name"],
		"bio":        record["bio"],
		"location_user": map[string]interface{}{
			"lat": latitude,
			"lon": longitude,
		},
		"updated_at": record["updated_at"],
	}

	data, err := json.Marshal(esData)
	if err != nil {
		return nil, err
	}

	return data, nil
}
