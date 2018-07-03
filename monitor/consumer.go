package monitor

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"time"

	"opentracing-zipkin-demo/config"

	"github.com/bsm/sarama-cluster"
	"github.com/olivere/elastic"
)

type MonitorConsumer struct {
	consumer *cluster.Consumer
	cli      *elastic.Client
	bp       *elastic.BulkProcessor
	sign     chan os.Signal
}

func NewConsumer(conf config.Conf) *MonitorConsumer {
	// -----------------------------------------------------------
	// init (custom) config, enable errors and notifications
	c := cluster.NewConfig()
	c.Consumer.Return.Errors = true
	c.Group.Return.Notifications = true
	c.ChannelBufferSize = 2048
	c.Net.KeepAlive = 300
	c.Net.MaxOpenRequests = 256

	// init consumer
	consumer, err := cluster.NewConsumer(conf.KafkaBrokers, "my-consumer-group", []string{conf.KafkaTopic}, c)
	if err != nil {
		panic(err)
	}

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()

	// -----------------------------------------------------------
	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// -----------------------------------------------------------
	// elasticsearch insert
	opts := []elastic.ClientOptionFunc{
		elastic.SetSniff(false),
		elastic.SetURL(conf.ESEndPoint...),
	}

	cli, err := elastic.NewClient(opts...)
	if err != nil {
		panic(err)
	}

	bp, err := cli.BulkProcessor().
		Workers(conf.ESWorkers).
		BulkActions(conf.ESBulkActions).
		BulkSize(conf.ESBulkSize).
		FlushInterval(time.Duration(conf.ESFlushInterval) * time.Second).Do(context.Background())

	if err != nil {
		panic(err)
	}

	// -----------------------------------------------------------
	// return MonitorConsumer
	return &MonitorConsumer{
		consumer: consumer,
		cli:      cli,
		bp:       bp,
		sign:     signals,
	}
}

func (mc *MonitorConsumer) Run(conf config.Conf) {
	log.Println(">>> kafka consumer worker staring ...")
	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-mc.consumer.Messages():
			if ok {
				acLog := new(AccessLogEntry)
				if err := json.Unmarshal(msg.Value, acLog); err != nil {
					log.Println(err)
					mc.consumer.MarkOffset(msg, "")
					break
				}

				r := elastic.NewBulkIndexRequest().Index(conf.KafkaTopic).Type("doc").Doc(acLog)
				mc.bp.Add(r)

				mc.consumer.MarkOffset(msg, "") // mark message as processed
			}
		case <-mc.sign:
			log.Println("Byebye, I will come back!!!")
			return
		}
	}
}

func (mc *MonitorConsumer) Finished() {
	defer mc.bp.Close()
	defer mc.cli.Stop()
	defer mc.consumer.Close()
}
