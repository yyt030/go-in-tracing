package config

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"strings"
)

type Conf struct {
	Profile             bool     `json:"profile"`
	ZipkinEndPoint      string   `json:"zipkin_http_endpoint"`
	RequestURL          string
	MaxIdleConnsPerHost int      `json:"max_idle_conns_per_host"`
	BrokerList          []string `json:"broker_list"`
	Topic               string   `json:"topic"`
	MaxKafkaMsgLen      int      `json:"max_kafka_msg_len"`
	MaxTracingMsgLen    int      `json:"max_tracing_msg_len"`
}

// Read config file
func ReadFile(filename string) (*Conf, error) {
	text, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	cf := new(Conf)
	if err := json.Unmarshal(text, cf); err != nil {
		return nil, err
	}
	return cf, nil
}

func (c *Conf) VerifyConfig() error {
	if c.BrokerList == nil {
		return errors.New("BrokerList must config")
	}
	if c.ZipkinEndPoint == "" {
		return errors.New("ZipkinEndPoint must config")
	}

	if c.RequestURL != "" && !strings.HasPrefix(c.RequestURL, "http://") {
		return errors.New("backend must prefix 'http://' protocol scheme")
	}
	return nil
}
