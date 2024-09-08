package nsq

import (
	"context"
	"encoding/json"
	"github.com/avast/retry-go/v4"
	"github.com/go-estar/config"
	"github.com/go-estar/logger"
	"github.com/nsqio/go-nsq"
	"reflect"
	"time"
)

type ProducerConfig struct {
	NsqdAddr       string
	NsqLogger      logger.Logger
	MsgLogger      logger.Logger
	MsgLoggerLevel string
}

type ProducerOption func(*ProducerConfig)

func ProducerWithNsqdAddr(val string) ProducerOption {
	return func(opts *ProducerConfig) {
		opts.NsqdAddr = val
	}
}

func defaultProducerConfig(c *config.Config) *ProducerConfig {
	return &ProducerConfig{
		NsqdAddr:  c.GetString("nsq.nsqdAddr"),
		NsqLogger: logger.NewZapWithConfig(c, "nsq-producer", "error"),
		MsgLogger: logger.NewZapWithConfig(c, "mq-producer", "info"),
	}
}

func NewProducerWithConfig(c *config.Config, opts ...ProducerOption) *Producer {
	defaultConfig := defaultProducerConfig(c)
	for _, apply := range opts {
		if apply != nil {
			apply(defaultConfig)
		}
	}
	return NewProducer(defaultConfig)
}

func NewProducer(c *ProducerConfig) *Producer {

	if c == nil {
		panic("config 必须设置")
	}
	if c.NsqdAddr == "" {
		panic("NsqdAddr 必须设置")
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

	nsqConfig := nsq.NewConfig()
	producer, err := nsq.NewProducer(c.NsqdAddr, nsqConfig)
	if err != nil {
		panic(err)
	}

	producer.SetLogger(Logger(c.NsqLogger), Level(c.NsqLogger))

	return &Producer{
		c, producer,
	}
}

type Producer struct {
	*ProducerConfig
	*nsq.Producer
}

func (p *Producer) MessageFormat(data interface{}) ([]byte, error) {
	var body []byte
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Type() == reflect.TypeOf(body) {
		body = data.([]byte)
	} else {
		if reflect.TypeOf(data).Kind() == reflect.String {
			body = []byte(data.(string))
		} else {
			return json.Marshal(data)
		}
	}
	return body, nil
}

func (p *Producer) SendSync(topic string, data interface{}, opts ...SendOption) (err error) {
	c := &SendConfig{}
	for _, apply := range opts {
		apply(c)
	}

	body, err := p.MessageFormat(data)
	defer func() {
		if err != nil || p.MsgLoggerLevel == "info" {
			p.MsgLogger.Info(string(body),
				p.MsgLogger.Field("topic", topic),
				p.MsgLogger.Field("error", err),
			)
		}
	}()
	if err != nil {
		return err
	}

	if c.LocalRetry != nil {
		if c.RetryAttempts == 0 {
			c.RetryAttempts = 1
		}
		return retry.Do(
			func() error {
				if c.Delay != 0 {
					return p.DeferredPublish(topic, c.Delay, body)
				} else {
					return p.Publish(topic, body)
				}
			},
			retry.Attempts(c.RetryAttempts),
			retry.DelayType(c.LocalRetry),
			retry.LastErrorOnly(true),
		)
	}
	if c.Delay != 0 {
		return p.DeferredPublish(topic, c.Delay, body)
	} else {
		return p.Publish(topic, body)
	}
}

func (p *Producer) SimpleSendSync(topic string, data string) (err error) {
	return p.Publish(topic, []byte(data))
}

func (p *Producer) DeferredSendSync(topic string, delay time.Duration, data interface{}, opts ...SendOption) error {
	opts = append(opts, WithDelay(delay))
	return p.SendSync(topic, data, opts...)
}

type SendConfig struct {
	Ctx           context.Context
	Delay         time.Duration
	RetryAttempts uint
	LocalRetry    retry.DelayTypeFunc
}

type SendOption func(*SendConfig)

func WithCtx(val context.Context) SendOption {
	return func(opts *SendConfig) {
		opts.Ctx = val
	}
}
func WithDelay(val time.Duration) SendOption {
	return func(opts *SendConfig) {
		opts.Delay = val
	}
}
func WithLocalRetry(localRetry retry.DelayTypeFunc, attempts uint) SendOption {
	return func(opts *SendConfig) {
		opts.LocalRetry = localRetry
		opts.RetryAttempts = attempts
	}
}
