package nsq

import (
	"context"
	"github.com/go-estar/logger"
	"strconv"
	"testing"
	"time"
)

func TestProducer(t *testing.T) {
	//_, closer := tracer.NewJaeger("nsq-test", "127.0.0.1:9411")
	//defer closer.Close()

	producer := NewProducer(&ProducerConfig{
		NsqdAddr:  "127.0.0.1:4150",
		NsqLogger: logger.NewZap("nsq-producer", "error"),
		MsgLogger: logger.NewZap("mq-producer", "info"),
	})

	for i := 0; i < 1; i++ {
		go func(int2 int) {
			producer.SendSync("test-topic1", []byte("hi"+strconv.Itoa(int2)), WithCtx(context.Background()), WithDefaultLocalRetry(3))
		}(i)
	}

	time.Sleep(time.Hour)
}
