package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"opentracing-zipkin-demo/config"

	"github.com/bsm/sarama-cluster"
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

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				consumer.MarkOffset(msg, "") // mark message as processed
			}
		case <-signals:
			return
		}
	}
}
