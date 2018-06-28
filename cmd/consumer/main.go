package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"opentracing-zipkin-demo/config"
	"opentracing-zipkin-demo/monitor"

	"github.com/bsm/sarama-cluster"
	"github.com/olivere/elastic"
)

var (
	cf = flag.String("conf", "config.json", "config file name")
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}

func main() {
	flag.Parse()

	conf, err := config.ReadFile(*cf)
	if err != nil {
		flag.PrintDefaults()
		log.Fatal("read config file failed:", err)
	}

	if err = conf.VerifyConfig(); err != nil {
		flag.PrintDefaults()
		log.Fatal(err)
	}

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
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

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

	// elasticsearch insert
	opts := []elastic.ClientOptionFunc{
		elastic.SetSniff(false),
		elastic.SetURL(conf.ESEndPoint...),
	}

	cli, err := elastic.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer cli.Stop()

	ctx := context.Background()
	defer ctx.Done()

	p, err := cli.BulkProcessor().
		Workers(conf.ESWorkers).
		BulkActions(conf.ESBulkActions).
		BulkSize(conf.ESBulkSize).
		FlushInterval(time.Duration(conf.ESFlushInterval) * time.Second).Do(ctx)

	if err != nil {
		panic(err)
	}
	defer p.Close()

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				acLog := new(monitor.AccessLogEntry)
				if err := json.Unmarshal(msg.Value, acLog); err != nil {
					fmt.Println(err)
					consumer.MarkOffset(msg, "")
					break
				}

				r := elastic.NewBulkIndexRequest().Index(conf.KafkaTopic).Type("doc").Doc(acLog)
				p.Add(r)

				//fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				consumer.MarkOffset(msg, "") // mark message as processed
			}
		case <-signals:
			return
		}
	}
}
