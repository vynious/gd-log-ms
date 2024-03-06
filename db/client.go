package db

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"time"
)

type Repository struct {
	mg       *mongo.Client
	timeout  time.Duration
	dbname   string
	collname string
}

type Config struct {
	Url      string
	DBName   string
	CollName string
}

func LoadMongoConfig() Config {
	uri := os.Getenv("MONGO_CONN_URI")
	dbname := os.Getenv("MONGO_DB_NAME")
	collname := os.Getenv("MONGO_COLL_NAME")
	if uri == "" || dbname == "" || collname == "" {
		log.Fatalf("please check environment variables")
	}
	return Config{
		Url:      uri,
		DBName:   dbname,
		CollName: collname,
	}
}

func SpawnRepository(cfg Config) (*Repository, error) {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(cfg.Url))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mongodb: %w", err)
	}
	return &Repository{
		mg:       client,
		timeout:  time.Duration(2) * time.Second,
		dbname:   cfg.DBName,
		collname: cfg.CollName,
	}, nil
}

func (r *Repository) SaveLog(requestId string, message string) error {
	entry := logrus.WithFields(
		logrus.Fields{
			"requestId": requestId,
			"message":   message,
		})

	log.Println(entry)

	_, err := r.mg.Database(r.dbname).Collection(r.collname).InsertOne(context.TODO(), bson.D{
		{"RequestID", requestId},
		{"LogDetails", entry},
	})
	if err != nil {
		return fmt.Errorf("error storing log: %w", err)
	}
	return nil
}

func (r *Repository) CloseConnection() error {
	if err := r.mg.Disconnect(context.Background()); err != nil {
		return fmt.Errorf("failed to close mongodb %w", err)
	}
	return nil
}
