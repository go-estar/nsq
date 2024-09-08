package main

import (
	"errors"
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"time"
)

type Retry struct {
	DefaultRequeueDelay time.Duration
	MaxRequeueDelay     time.Duration
	MaxAttempts         uint16
}

func NewRetry(defaultRequeueDelay time.Duration, maxRequeueDelay time.Duration, maxAttempts uint16) *Retry {
	return &Retry{
		defaultRequeueDelay, maxRequeueDelay, maxAttempts,
	}
}
func defaultRetry() *Retry {
	return NewRetry(time.Second*6, time.Second*120, 30)
}

type BackoffStrategy struct {
	cfg *nsq.Config
}

func (s *BackoffStrategy) Calculate(attempt int) time.Duration {
	backoffDuration := time.Second * 6
	return backoffDuration
}

type NoneBackoffStrategy struct {
	cfg *nsq.Config
}

func (s *NoneBackoffStrategy) Calculate(attempt int) time.Duration {
	backoffDuration := time.Duration(0)
	return backoffDuration
}

type MyMessageHandler1 struct {
}

//HandleMessage implements the Handler interface.
func (h *MyMessageHandler1) HandleMessage(m *nsq.Message) error {
	if len(m.Body) == 0 {
		//Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		return nil
	}

	fmt.Printf("%v %v---%v\n", time.Now().Format("2006-01-02 15:04:05.000"), m.Attempts, string(m.Body))
	//return nil
	return errors.New("err")
	//Returning a non-nil error will automatically send a REQ command to NSQ to re-queue the message.
}

func main() {
	//Instantiate a consumer that will subscribe to the provided channel.
	config := nsq.NewConfig()
	config.BackoffStrategy = &NoneBackoffStrategy{}

	retry := defaultRetry()
	config.DefaultRequeueDelay = retry.MaxRequeueDelay
	config.MaxRequeueDelay = retry.MaxRequeueDelay
	config.MaxAttempts = retry.MaxAttempts

	consumer, err := nsq.NewConsumer("test", "channel-1", config)
	if err != nil {
		log.Println(err)
	}

	//Set the Handler for messages received by this Consumer. Can be called multiple times.
	//See also AddConcurrentHandlers.
	consumer.AddHandler(&MyMessageHandler1{})

	//Use nsqlookupd to discover nsqd instances.
	//See also ConnectToNSQD, ConnectToNSQDs, ConnectToNSQLookupds.
	err = consumer.ConnectToNSQLookupd("localhost:4161")
	if err != nil {
		log.Println(err)
	}

	time.Sleep(time.Hour)
	//Gracefully stop the consumer.
	//consumer.Stop()
}
