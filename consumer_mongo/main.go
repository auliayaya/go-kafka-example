package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func getMongoCollection(mongoUrl, dbName, colName string) *mongo.Collection {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://docker:mongopw@localhost:49154"))
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB....!")
	db := client.Database(dbName)
	collection := db.Collection(colName)
	return collection
}

func getKafkaReader(kafkaUrl, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaUrl},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
}

func main() {

	mongoUrl := os.Getenv("MONGO_URL")
	dbName := "kafka-go"
	colName := "product"
	collection := getMongoCollection(mongoUrl, dbName, colName)

	kafkaUrl := os.Getenv("KAFKA_URL")
	topic := "first_kafka_topic"
	groupID := "logger_mongo"
	reader := getKafkaReader(kafkaUrl, topic, groupID)

	defer reader.Close()

	fmt.Println("Start Consuming..... !!!")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		inserResult, err := collection.InsertOne(context.Background(), msg)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Inserted a single document : ", inserResult.InsertedID)
	}
}
