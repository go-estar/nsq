package nsq

import (
	"context"
	"errors"
	"fmt"
	"github.com/avast/retry-go/v4"
	baseError "github.com/go-estar/base-error"
	"github.com/go-estar/config"
	"github.com/go-estar/logger"
	"github.com/jinzhu/copier"
	"github.com/nsqio/go-nsq"
	"time"
)

type LocalRetry struct {
	DelayTypeFunc retry.DelayTypeFunc
	Attempts      uint
}

func NewLocalRetry(defaultDelay time.Duration, maxDelay time.Duration, attempts uint) *LocalRetry {
	return &LocalRetry{
		Attempts: attempts,
		DelayTypeFunc: func(n uint, err error, config *retry.Config) time.Duration {
			delay := defaultDelay * time.Duration(n+1)
			if delay > maxDelay {
				delay = maxDelay
			}
			return delay
		},
	}
}

func NewDefaultLocalRetry(attempts uint) *LocalRetry {
	return NewLocalRetry(time.Second, time.Second*10, attempts)
}

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

func NewDefaultRetry(attempts uint16) *Retry {
	return NewRetry(time.Second*10, time.Second*10, attempts)
}

func defaultRetry() *Retry {
	return NewRetry(time.Second*60, time.Minute*10, 10)
}

type Handler = func(ctx context.Context, msg []byte, finished bool) error

type ConsumerConfig struct {
	NsqLookUpAddr   string
	NsqdAddr        string
	Broadcasting    bool
	Channel         string
	Name            string
	ServerId        string
	Topic           string
	Concurrent      bool
	MaxInFlight     int
	LocalRetry      *LocalRetry
	Retry           *Retry
	RetryAttempts   uint16
	RetryStrategy   func(attempts uint16) (delay time.Duration)
	BackoffDisabled bool
	NsqLogger       logger.Logger
	MsgLogger       logger.Logger
	MsgLoggerLevel  string
	Handler         Handler
	SimpleHandler   func([]byte) error
}

type ConsumerOption func(*ConsumerConfig)

func ConsumerWithNsqLookUpAddr(val string) ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.NsqLookUpAddr = val
	}
}
func ConsumerWithBroadcasting() ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.Broadcasting = true
	}
}
func ConsumerWithName(val string) ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.Name = val
	}
}
func ConsumerWithChannel(val string) ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.Channel = val
	}
}
func ConsumerWithServerId(val string) ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.ServerId = val
	}
}
func ConsumerWithTopic(val string) ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.Topic = val
	}
}
func ConsumerWithRetry(val *Retry) ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.Retry = val
	}
}
func ConsumerWithBackoffDisabled() ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.BackoffDisabled = true
	}
}
func ConsumerWithHandler(val Handler) ConsumerOption {
	return func(opts *ConsumerConfig) {
		opts.Handler = val
	}
}

func defaultConsumerConfig(c *config.Config) *ConsumerConfig {
	return &ConsumerConfig{
		NsqLookUpAddr: c.GetString("nsq.nsqLookUpAddr"),
		NsqdAddr:      c.GetString("nsq.nsqdAddr"),
		Channel:       c.GetString("application.name"),
		ServerId:      c.GetString("cluster.podName"),
		NsqLogger:     logger.NewZapWithConfig(c, "nsq-consumer", "error"),
		MsgLogger:     logger.NewZapWithConfig(c, "mq-consumer", "info"),
	}
}

func NewConsumerWithConfig(c *config.Config, consumerConfig *ConsumerConfig, opts ...ConsumerOption) (*Consumer, error) {
	defaultConfig := defaultConsumerConfig(c)
	if err := copier.CopyWithOption(defaultConfig, consumerConfig, copier.Option{
		IgnoreEmpty: true,
	}); err != nil {
		return nil, err
	}
	for _, apply := range opts {
		if apply != nil {
			apply(defaultConfig)
		}
	}
	return NewConsumer(defaultConfig)
}

func NewConsumer(c *ConsumerConfig) (*Consumer, error) {
	if c == nil {
		return nil, errors.New("config 必须设置")
	}
	if c.NsqLookUpAddr == "" && c.NsqdAddr == "" {
		return nil, errors.New("NsqLookUpAddr或NsqdAddr必须设置其一")
	}
	if c.Channel == "" {
		return nil, errors.New("Channel 必须设置")
	}
	if c.Topic == "" {
		return nil, errors.New("Topic 必须设置")
	}
	if c.Handler == nil && c.SimpleHandler == nil {
		return nil, errors.New("Handler 必须设置")
	}
	if c.NsqLogger == nil {
		panic("NsqLogger 必须设置")
	}
	if c.MsgLogger == nil {
		panic("MsgLogger 必须设置")
	}
	if c.MsgLoggerLevel == "" {
		c.MsgLoggerLevel = "error"
	}

	if c.Name != "" {
		c.Channel = c.Channel + "-" + c.Name
	}
	if c.Broadcasting {
		c.Channel = c.Channel + "-" + c.ServerId
	}

	nsqConfig := nsq.NewConfig()

	if c.BackoffDisabled {
		nsqConfig.MaxBackoffDuration = 0
	}

	if c.Retry == nil {
		c.Retry = defaultRetry()
	}
	if c.RetryAttempts > 0 {
		c.Retry.MaxAttempts = c.RetryAttempts
	}
	nsqConfig.DefaultRequeueDelay = c.Retry.DefaultRequeueDelay
	nsqConfig.MaxRequeueDelay = c.Retry.MaxRequeueDelay
	nsqConfig.MaxAttempts = c.Retry.MaxAttempts

	if c.MaxInFlight > 0 {
		nsqConfig.MaxInFlight = c.MaxInFlight
	}
	consumer, err := nsq.NewConsumer(c.Topic, c.Channel, nsqConfig)
	if err != nil {
		return nil, err
	}

	consumer.SetLogger(Logger(c.NsqLogger), Level(c.NsqLogger))

	if c.Handler != nil {
		h := nsq.HandlerFunc(func(m *nsq.Message) error {
			var ignoreErr error
			var handlerErr error
			var startTime = time.Now()
			defer func() {
				latency := time.Since(startTime).Milliseconds()
				if handlerErr != nil || c.MsgLoggerLevel == "info" {
					c.MsgLogger.Info(
						string(m.Body),
						logger.NewField("startTime", startTime),
						logger.NewField("latency", latency),
						logger.NewField("topic", c.Topic),
						logger.NewField("channel", c.Channel),
						logger.NewField("id", fmt.Sprintf("%s", m.ID)),
						logger.NewField("attempts", m.Attempts),
						logger.NewField("ignore_err", ignoreErr),
						logger.NewField("error", handlerErr),
					)
				}
			}()

			finished := nsqConfig.MaxAttempts == m.Attempts

			if c.LocalRetry != nil {
				handlerErr = retry.Do(
					func() error {
						return c.Handler(context.Background(), m.Body, finished)
					},
					retry.Attempts(c.LocalRetry.Attempts),
					retry.DelayType(c.LocalRetry.DelayTypeFunc),
					retry.LastErrorOnly(true),
					retry.RetryIf(func(err error) bool {
						return err != nil && !baseError.IsNotSystemError(err)
					}),
				)
			} else {
				handlerErr = c.Handler(context.Background(), m.Body, finished)
			}

			if handlerErr != nil {
				if baseError.IsNotSystemError(handlerErr) {
					ignoreErr = handlerErr
					handlerErr = nil
				}
				if c.RetryStrategy != nil {
					if handlerErr != nil && !finished {
						delay := c.RetryStrategy(m.Attempts)
						if delay == 0 {
							delay = time.Minute
						}
						m.RequeueWithoutBackoff(delay)
					}
					return nil
				}
				return handlerErr
			}
			return nil
		})
		if c.Concurrent {
			consumer.AddConcurrentHandlers(h, c.MaxInFlight)
		} else {
			consumer.AddHandler(h)
		}
	}

	if c.SimpleHandler != nil {
		consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) (err error) {
			return c.SimpleHandler(m.Body)
		}))
	}

	if c.NsqLookUpAddr != "" {
		if err = consumer.ConnectToNSQLookupd(c.NsqLookUpAddr); err != nil {
			return nil, err
		}
	} else {
		if err = consumer.ConnectToNSQD(c.NsqdAddr); err != nil {
			return nil, err
		}
	}

	return &Consumer{
		c, consumer,
	}, nil
}

type Consumer struct {
	*ConsumerConfig
	*nsq.Consumer
}

func (c *Consumer) Disconnect() error {
	c.Stop()
	return c.DisconnectFromNSQLookupd(c.NsqLookUpAddr)
}

var defaultRetryStrategyDelays = []time.Duration{
	0,
	time.Second * 5,
	time.Second * 5,
	time.Second * 10,
	time.Second * 30,
	time.Minute,
	time.Minute * 5,
	time.Minute * 10,
	time.Minute * 30,
	time.Hour,
	time.Hour * 2,
}

func DefaultRetryStrategy(attempts uint16) (delay time.Duration) {
	var i = int(attempts)
	if i > len(defaultRetryStrategyDelays)-1 {
		i = len(defaultRetryStrategyDelays) - 1
	}
	return defaultRetryStrategyDelays[i]
}
