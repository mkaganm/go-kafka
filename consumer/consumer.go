package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "example-topic"

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to start consumer for partition: %v", err)
	}
	defer partitionConsumer.Close()

	// Create a channel to catch OS signals
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)

	fmt.Println("Consumer is ready to receive messages...")

	// Read messages
	go func() {
		for msg := range partitionConsumer.Messages() {
			fmt.Printf("Received message: %s\n", string(msg.Value))
		}
	}()

	// Wait for OS signals
	<-sigchan
	fmt.Println("Shutting down consumer...")
}
