package main

import (
	"context"
	"flag"
	"io/ioutil"
	"log"
	"sync"

	"opentracing-zipkin-demo/config"
	"opentracing-zipkin-demo/monitor"

	"github.com/olivere/elastic"
)

var (
	cf = flag.String("conf", "config.json", "config file name")
	im = flag.String("index_mapping", "mapping.json", "index mapping file")
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}

func IndexExists(c config.Conf) error {
	ctx := context.Background()
	defer ctx.Done()

	// elasticsearch insert
	opts := []elastic.ClientOptionFunc{
		elastic.SetSniff(false),
		elastic.SetURL(c.ESEndPoint...),
	}

	cli, err := elastic.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cli.Stop()

	// Get mapping from json file
	contents, err := ioutil.ReadFile(*im)
	if err != nil {
		log.Println("error: can't not find index mapping file")
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := cli.IndexExists(c.KafkaTopic).Do(ctx)
	if err != nil {
		return err
	}
	if !exists {
		// Create a new index.
		createIndex, err := cli.CreateIndex(c.KafkaTopic).BodyString(string(contents)).Do(ctx)
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

func CreateWorker(c config.Conf) {
	log.Printf(">>> create workers:%d, %+v", c.KafkaWorkersNum, c)

	// Precreate index
	if err := IndexExists(c); err != nil {
		panic(err)
	}

	// Start workers
	var wg sync.WaitGroup
	wg.Add(c.KafkaWorkersNum)
	for i := 0; i < c.KafkaWorkersNum; i++ {
		mc := monitor.NewConsumer(c)
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
