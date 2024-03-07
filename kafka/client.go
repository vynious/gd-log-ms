package kafka

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"github.com/vynious/gd-log-ms/db"
	"log"
	"os"
)

type LogSubscriber struct {
	Store      *db.Repository
	Subscriber *kafka.Reader
}

func LoadKafkaConfigurations() kafka.ReaderConfig {
	kafkaUrl := os.Getenv("KAFKA_URL")
	if kafkaUrl == "" {
		log.Fatalf("missing url for kafka subscriber")
	}
	return kafka.ReaderConfig{
		Brokers: []string{
			kafkaUrl,
		},
		Topic: "logs",
	}
}

func SpawnLogSubscriber(cfg kafka.ReaderConfig, repo *db.Repository) *LogSubscriber {
	sub := kafka.NewReader(cfg)
	return &LogSubscriber{
		Subscriber: sub,
		Store:      repo,
	}
}

func (ls *LogSubscriber) Start() {

	for {
		msg, err := ls.Subscriber.ReadMessage(context.Background())
		if err != nil {
			log.Println("error reading kafka messages: ", err)
		}

		var logEntry struct {
			RequestId   string `json:"requestId"`
			Content     string `json:"content"`
			ContentType string `json:"contentType"`
		}

		if err := json.Unmarshal(msg.Value, &logEntry); err != nil {
			log.Println("failed to unmarshal msg: ", err)
			continue
		}

		if err = ls.Store.SaveLog(logEntry.RequestId, logEntry.ContentType, logEntry.Content); err != nil {
			log.Printf("failed to save store logs")
			continue
		}

	}

}

func (ls *LogSubscriber) CloseConnections() {
	if err := ls.Store.CloseConnection(); err != nil {
		log.Fatalf("error closing mongo: %v", err)
	}

	if err := ls.Subscriber.Close(); err != nil {
		log.Fatalf("error closing kafka connection: %v", err)
	}
}
