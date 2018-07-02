package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"opentracing-zipkin-demo/config"
	"opentracing-zipkin-demo/monitor"

	"github.com/bsm/sarama-cluster"
	"github.com/olivere/elastic"
)

var (
	cf = flag.String("conf", "config.json", "config file name")
)

type MonitorConsumer struct {
	consumer *cluster.Consumer
	cli      *elastic.Client
	bp       *elastic.BulkProcessor

	sign chan os.Signal
	//conf config.Conf
}

const indexMapping = `
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
	"refresh_interval":"5s",
	"translog.durability":"async",
	"translog.flush_threshold_size": "512m"
  },
  "mappings": {
    "doc": {
      "properties": {
        "@timestamp": {
          "type": "date",
          "format": "dateOptionalTime"
        },
        "method": {
          "type": "text"
        },
        "host": {
          "type": "text"
        },
        "path": {
          "type": "text"
        },
        "ip": {
          "type": "text"
        },
        "response_time": {
          "type": "text"
        },
        "at": {
          "type": "text"
        },
        "at2": {
          "type": "text"
        },
        "at3": {
          "type": "text"
        },
        "traceno": {
          "type": "text"
        },
        "message": {
          "type": "text"
        }
      }
    }
  }
}
`

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
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

func (mc *MonitorConsumer) IndexExists(indexName string) error {
	// Use the IndexExists service to check if a specified index exists.
	exists, err := mc.cli.IndexExists(indexName).Do(context.Background())
	if err != nil {
		return err
	}
	if !exists {
		// Create a new index.
		createIndex, err := mc.cli.CreateIndex(indexName).BodyString(indexMapping).Do(context.Background())
		if err != nil {
			// Handle error
			return err
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}

	return nil
}

func (mc *MonitorConsumer) Run(conf config.Conf) {
	log.Println(">>> kafka consumer worker staring ...")
	// check elastic index
	if err := mc.IndexExists(conf.KafkaTopic); err != nil {
		log.Println(err)
	}

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-mc.consumer.Messages():
			if ok {
				acLog := new(monitor.AccessLogEntry)
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

func CreateWorker(c config.Conf) {
	log.Printf(">>> create workers:%d, %+v", c.KafkaWorkersNum, c)
	var wg sync.WaitGroup
	wg.Add(c.KafkaWorkersNum)
	for i := 0; i < c.KafkaWorkersNum; i++ {
		mc := NewConsumer(c)
		go func() {
			mc.Run(c)
			mc.Finished()
			wg.Done()
		}()
	}
	wg.Wait()
}

func main() {
	flag.Parse()

	c, err := config.ReadFile(*cf)
	if err != nil {
		flag.PrintDefaults()
		log.Fatal("read config file failed:", err)
	}

	if err = c.VerifyConfig(); err != nil {
		flag.PrintDefaults()
		log.Fatal(err)
	}

	CreateWorker(*c)
}
