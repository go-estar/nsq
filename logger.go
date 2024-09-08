package nsq

import (
	"github.com/go-estar/logger"
	"github.com/nsqio/go-nsq"
	"strings"
)

type LoggerFunc func(msg string, fields ...*logger.Field)

func (l LoggerFunc) Output(calldepth int, s string) error {
	if !strings.Contains(s, "TOPIC_NOT_FOUND") {
		l(s)
	}
	return nil
}

func Logger(l logger.Logger) LoggerFunc {
	var nsqLogger LoggerFunc
	switch strings.ToLower(l.Level()) {
	case "debug":
		nsqLogger = l.Debug
	case "warn":
		nsqLogger = l.Warn
	case "error":
		nsqLogger = l.Error
	default:
		nsqLogger = l.Info
	}
	return nsqLogger
}

func Level(l logger.Logger) nsq.LogLevel {
	var nsqLevel nsq.LogLevel
	switch strings.ToLower(l.Level()) {
	case "debug":
		nsqLevel = nsq.LogLevelDebug
	case "warn":
		nsqLevel = nsq.LogLevelWarning
	case "error":
		nsqLevel = nsq.LogLevelError
	default:
		nsqLevel = nsq.LogLevelInfo
	}
	return nsqLevel
}
