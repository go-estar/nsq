package nsq

import (
	"github.com/go-estar/config"
	"github.com/go-resty/resty/v2"
)

func CreateTopic(topic string, addr string) string {
	_, err := resty.New().R().
		SetQueryParams(map[string]string{
			"topic": topic,
		}).
		Post("http://" + addr + "/topic/create")
	if err != nil {
		panic(err)
	}
	return topic
}

func CreateTopicWithConfig(topic string, c *config.Config) string {
	_, err := resty.New().R().
		SetQueryParams(map[string]string{
			"topic": topic,
		}).
		Post("http://" + c.GetString("nsq.nsqdHttp") + "/topic/create")
	if err != nil {
		panic(err)
	}
	return topic
}
