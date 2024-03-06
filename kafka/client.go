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
			log.Fatalf("error reading kafka messages: %v", err)
		}

		log.Printf("%v", err)

		var logEntry struct {
			requestId string
			message   string
		}

		if err := json.Unmarshal(msg.Value, &logEntry); err != nil {
			log.Println("failed to unmarshal msg: ", err)
			continue
		}

		if err = ls.Store.SaveLog(logEntry.requestId, logEntry.message); err != nil {
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
