package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	kafka "github.com/segmentio/kafka-go"
)

func getKafkaReader(kafkaUrl, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaUrl, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
}
func main() {
	kafkaUrl := os.Getenv("KAFKA_URL")
	topic := "first_kafka_topic"
	groupID := "logger-group"

	reader := getKafkaReader(kafkaUrl, topic, groupID)
	defer reader.Close()

	fmt.Println("Start consuming....!!!")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("message at topic: %v partition:%v offset:%v %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}
