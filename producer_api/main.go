package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/segmentio/kafka-go"
)

func producerHandler(kafkaWriter *kafka.Writer) func(http.ResponseWriter, *http.Request) {
	return http.HandlerFunc(func(wrt http.ResponseWriter, req *http.Request) {
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Fatalln(err)
		}
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("address-%s", req.RemoteAddr)),
			Value: body,
		}
		err = kafkaWriter.WriteMessages(req.Context(), msg)
		if err != nil {
			wrt.Write([]byte(err.Error()))
			log.Fatalln(err)
		}
	})
}

func getKafkaWriter(kafkaUrl, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaUrl),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}
func main() {
	kafkaUrl := "localhost:9092"
	kafkaTopic := "first_kafka_topic"

	kafkaWriter := getKafkaWriter(kafkaUrl, kafkaTopic)
	defer kafkaWriter.Close()

	http.HandleFunc("/", producerHandler(kafkaWriter))

	fmt.Println("Start producer-api .... !!!")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
