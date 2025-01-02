package es

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type ES struct {
	Cl    *elasticsearch.Client
	Bi    esutil.BulkIndexer
	Index string
}

func NewClient(cloudID string, index string, apiKey string) (*elasticsearch.Client, esutil.BulkIndexer, string, error) {
	cfg := elasticsearch.Config{
		APIKey: apiKey,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
		CloudID: cloudID,
	}

	cl, err := elasticsearch.NewClient(cfg)

	if err != nil {
		return nil, nil, "", err
	}

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      index,
		Client:     cl,
		NumWorkers: 10,
	})

	if err != nil {
		return nil, nil, "", err
	}

	mapping := `{
		"mappings": {
			"properties": {
				"location_user": {
					"type": "geo_point"
				}
			}
		}
	}`

	_, err = cl.Indices.Create(index, cl.Indices.Create.WithBody(strings.NewReader(mapping)))

	if err != nil {
		return nil, nil, "", err
	}

	return cl, bi, index, nil
}

func (e *ES) IndexData(msg []byte) error {
	err := e.Bi.Add(context.Background(), esutil.BulkIndexerItem{
		Action: "index",
		Body:   bytes.NewReader(msg),
		OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {

		},
		OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
			if err != nil {
				fmt.Printf("Error adding document: %v\n", err)
			} else {
				fmt.Printf("Elasticsearch error: %s\n", res.Error.Reason)
			}
		},
		Index: e.Index,
	})
	if err != nil {
		return err
	}
	return nil
}

type LocationUser struct {
	Lon float32 `json:"lon"`
	Lan float32 `json:"lat"`
}

type User struct {
	FirstName    string       `json:"first_name"`
	LastName     string       `json:"last_name"`
	Bio          string       `json:"bio"`
	LocationUser LocationUser `json:"location_user"`
	UpdatedAt    time.Time    `json:"updated_at"`
}

type ESSearchResponse struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Hits     struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore float64       `json:"max_score"`
		Hits     []ESSearchHit `json:"hits"`
	} `json:"hits"`
}

type ESSearchHit struct {
	Index  string  `json:"_index"`
	ID     string  `json:"_id"`
	Score  float64 `json:"_score"`
	Source User    `json:"_source"`
}

func (e *ES) RetrieveUserFilteredData(index string, userLat string, userLong string, distance string) ([]ESSearchHit, error) {
	query := fmt.Sprintf(`{
        "query": {
            "geo_distance": {
                "distance": %s,
                "location_user": {
                    "lat": %s,
                    "lon": %s
                }
            }
        }
    }`, distance, userLat, userLong)

	res, err := e.Cl.Search(
		e.Cl.Search.WithIndex(index),
		e.Cl.Search.WithBody(strings.NewReader(query)),
	)
	if err != nil {
		return nil, fmt.Errorf("error executing search request: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error in response: %s", res.String())
	}

	var searchResult ESSearchResponse
	if err := json.NewDecoder(res.Body).Decode(&searchResult); err != nil {
		return nil, fmt.Errorf("error parsing response body: %v", err)
	}

	return searchResult.Hits.Hits, nil
}
