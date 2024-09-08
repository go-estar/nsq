package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"time"
)

func main() {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer("127.0.0.1:4150", config)
	if err != nil {
		log.Println(err)
	}

	topicName := "test"

	//Synchronously publish a single message to the specified topic.
	//Messages can also be sent asynchronously and/or in batches.
	for i := 0; i < 6; i++ {
		messageBody := []byte(fmt.Sprintf("hello %v", i))
		if err := producer.DeferredPublish(topicName, time.Second*30, messageBody); err != nil {
			log.Println("err==============", err)
		}
	}

	time.Sleep(time.Hour)
	//Gracefully stop the producer.
	producer.Stop()
}
