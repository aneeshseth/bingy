package main

import (
	"binge/cdc"
	"binge/es"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

func main() {
	_, bi, index, err := es.NewClient(os.Getenv("CLOUD_ID_ES"), "users", os.Getenv("API_KEY_ES"))
	if err != nil {
		panic(err)
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "binge-users-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}

	topic := "dbserver1.binge.users"
	c.SubscribeTopics([]string{topic}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Received Message: %s\n", string(msg.Value))
			var kafkaMessage map[string]interface{}
			if err := json.Unmarshal(msg.Value, &kafkaMessage); err != nil {
				log.Fatalf("Error unmarshaling message: %v\n", err)
			}

			payload, ok := kafkaMessage["payload"].(map[string]interface{})
			if !ok {
				log.Fatalf("Invalid payload format")
			}
			after, ok := payload["after"].(map[string]interface{})
			if !ok {
				log.Println("No 'after' field in payload; skipping")
				continue
			}

			esData, err := cdc.TransformCreateOperationForES(after)
			if err != nil {
				log.Fatalf("Error transforming data: %v\n", err)
			}

			err = bi.Add(context.Background(), esutil.BulkIndexerItem{
				Action: "index",
				Body:   bytes.NewReader(esData),
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {

				},
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						fmt.Printf("Error adding document: %v\n", err)
					} else {
						fmt.Printf("Elasticsearch error: %s\n", res.Error.Reason)
					}
				},
				Index: index,
			})

			if err != nil {
				panic(err)
			}

			fmt.Printf("Transformed and Ingested Elasticsearch Data: %s\n", string(esData))
		} else {
			fmt.Printf("Error: %v\n", err)
		}
	}
}
