package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func InitEnv() {
	err := godotenv.Load(".env")

	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

func MyEnv(str string) interface{} {
	InitEnv()
	return os.Getenv(str)
}

func ConnectMongo() *mongo.Database {
	InitEnv()
	clientOptions := options.Client()
	uri := os.Getenv("MONGODB_URI")
	clientOptions.ApplyURI(uri)

	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		log.Panic("mongo client not connected : " + err.Error())
	}

	err = client.Connect(context.Background())
	if err != nil {
		log.Panic("mongo connection failed: " + err.Error())
	}

	if err := client.Ping(context.TODO(), readpref.Primary()); err != nil {
		fmt.Println("mongodb failed...")
		log.Panic(err)
	}
	return client.Database("logs")
}
