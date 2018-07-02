package config

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"strings"
)

type Conf struct {
	MaxIdleConnsPerHost int `json:"http_max_idle_conns_per_host"`

	ZipkinEndPoint  string `json:"zipkin_endpoint"`
	ZipkinMaxMsgLen int    `json:"zipkin_max_msg_len"`

	KafkaWorkersNum int      `json:"kafka_workers_num"`
	KafkaBrokers    []string `json:"kafka_brokers"`
	KafkaTopic      string   `json:"kafka_topic"`
	KafkaMaxMsgLen  int      `json:"kafka_max_msg_len"`
	KafkaFlushFreq  int      `json:"kafka_flush_freq_ms"`

	ESEndPoint      []string `json:"es_endpoint"`
	ESWorkers       int      `json:"es_workers"`
	ESBulkActions   int      `json:"es_bulk_actions"`
	ESFlushInterval int      `json:"es_flush_interval"`
	ESBulkSize      int      `json:"es_bulk_size"`

	Profile    bool `json:"profile"`
	RequestURL string
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
	if c.KafkaBrokers == nil {
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
