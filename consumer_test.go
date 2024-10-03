package nsq

import (
	"context"
	"fmt"
	baseError "github.com/go-estar/base-error"
	"github.com/go-estar/logger"
	"testing"
	"time"
)

func TestConsumer(t *testing.T) {
	//_, closer := tracer.NewJaeger("nsq-test", "127.0.0.1:9411")
	//defer closer.Close()

	//var retryDelays = []time.Duration{
	//	0,
	//	time.Second * 1,
	//	time.Second * 2,
	//	time.Second * 3,
	//	time.Second * 4,
	//	time.Second * 5,
	//	time.Second * 6,
	//}
	var i = 0
	_, err := NewConsumer(&ConsumerConfig{
		NsqdAddr: "127.0.0.1:4150",
		Channel:  "test-01",
		Topic:    "test-topic1",
		//Retry:       NewRetry(time.Second*1, time.Second*1, 1),
		MaxInFlight: 200,
		Concurrent:  true,
		Retry:       NewDefaultRetry(3),
		LocalRetry:  NewDefaultLocalRetry(3),
		//RetryStrategy: func(attempts uint16) (delay time.Duration) {
		//	var i = int(attempts)
		//	if i > len(retryDelays)-1 {
		//		i = len(retryDelays) - 1
		//	}
		//	return retryDelays[i]
		//},
		BackoffDisabled: true,
		NsqLogger:       logger.NewZap("nsq-consumer", "error"),
		MsgLogger:       logger.NewZap("mq-consumer", "info"),
		Handler: func(ctx context.Context, msg []byte, finished bool) error {
			i++
			fmt.Println(time.Now().String(), i, string(msg))
			return baseError.NewCode("1", "asd", baseError.WithSystem())
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("start consumer")
	time.Sleep(time.Hour)
}

func TestRetryTime(t *testing.T) {
	r := NewRetry(time.Second*60, time.Minute*10, 20)
	var i time.Duration = 1
	var total time.Duration = 0
	for uint16(i) <= r.MaxAttempts {
		var v time.Duration = 0
		if r.DefaultRequeueDelay*i > r.MaxRequeueDelay {
			v = r.MaxRequeueDelay
		} else {
			v = r.DefaultRequeueDelay * i
		}
		t.Log(i, v.Minutes())
		total += v
		i++
	}
	t.Log("total", total.Hours())
}
