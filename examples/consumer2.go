package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"time"
)

type MyMessageHandler2 struct{}

//HandleMessage implements the Handler interface.
func (h *MyMessageHandler2) HandleMessage(m *nsq.Message) error {
	if len(m.Body) == 0 {
		//Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		return nil
	}

	fmt.Printf("%v %v---%v\n", time.Now().Format("2006-01-02 15:04:05.000"), m.Attempts, string(m.Body))

	//Returning a non-nil error will automatically send a REQ command to NSQ to re-queue the message.
	return nil
}

func main() {
	//Instantiate a consumer that will subscribe to the provided channel.
	config := nsq.NewConfig()
	consumer, err := nsq.NewConsumer("test", "channel-1", config)
	if err != nil {
		log.Fatal(err)
	}

	//Set the Handler for messages received by this Consumer. Can be called multiple times.
	//See also AddConcurrentHandlers.
	consumer.AddHandler(&MyMessageHandler2{})

	//Use nsqlookupd to discover nsqd instances.
	//See also ConnectToNSQD, ConnectToNSQDs, ConnectToNSQLookupds.
	err = consumer.ConnectToNSQLookupd("localhost:4161")
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Hour)
	//Gracefully stop the consumer.
	//consumer.Stop()
}
