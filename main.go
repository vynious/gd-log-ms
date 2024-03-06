package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"github.com/vynious/gd-log-ms/db"
	"github.com/vynious/gd-log-ms/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading .env files")
	}

	// init database
	repository, err := db.SpawnRepository(db.LoadMongoConfig())
	if err != nil {
		log.Fatalf("failed to start repository")
	}

	sub := kafka.SpawnLogSubscriber(kafka.LoadKafkaConfigurations(), repository)
	go sub.Start()

	fmt.Println("starting to read...")
	// Block main goroutine until an OS signal is received
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals

	fmt.Println("Termination signal received, shutting down.")

	// Clean up resources
	sub.CloseConnections()
}
