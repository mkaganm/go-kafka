package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "example-topic"

	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	rand.Seed(time.Now().UnixNano()) // Seed the random number generator

	for i := 0; i < 10; i++ {
		randomNumber := rand.Intn(100) // Generate a random number between 0 and 99
		message := fmt.Sprintf("Hello, Kafka from Golang Producer! Random number: %d", randomNumber)
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(message),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Fatalf("Failed to send message: %v", err)
		}

		fmt.Printf("Message sent successfully to partition %d, offset %d\n", partition, offset)
	}
}
