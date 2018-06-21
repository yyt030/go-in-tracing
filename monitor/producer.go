package monitor

import (
	"log"
	"net/http"
	"time"

	"opentracing-zipkin-demo/config"

	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
	"github.com/satori/go.uuid"
)

type MockServer struct {
	Conf              config.Conf
	AccessLogProducer sarama.AsyncProducer
	Server            http.Server
	Tracer            opentracing.Tracer
}

func (ms *MockServer) Close() error {
	if err := ms.AccessLogProducer.Close(); err != nil {
		log.Println("Failed to shut down access log producer cleanly", err)
	}

	return nil
}

func (ms *MockServer) WithAccessLog() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()

		sp := NewSpan(r)
		defer sp.Finish()

		traceNo := uuid.NewV4()
		sp.SetTag("status", "N")
		sp.SetTag("traceNo", traceNo.String())

		sp.LogKV("message", RandString(ms.Conf.MaxTracingMsgLen))
		sp.LogKV("client_send", "AAA")
		if ms.Conf.RequestURL != "" {
			_, err := NewCliReq(ms.Conf, sp)
			if err != nil {
				log.Println("request service1 failed:", err)
			}
		}
		sp.LogKV("client_receive", "AAA")

		entry := &AccessLogEntry{
			Method:       r.Method,
			Host:         r.Host,
			Path:         r.RequestURI,
			IP:           r.RemoteAddr,
			ResponseTime: float64(time.Since(started)) / float64(time.Second),
			TraceNo:      traceNo.String(),
			Message:      RandString(ms.Conf.MaxKafkaMsgLen),
		}

		// We will use the client's IP address as key. This will cause
		// all the access log entries of the same IP address to end up
		// on the same partition.
		ms.AccessLogProducer.Input() <- &sarama.ProducerMessage{
			Topic: ms.Conf.Topic,
			Key:   sarama.StringEncoder(r.RemoteAddr),
			Value: entry,
		}

		w.Write([]byte("hello, world!"))
	})
}

func NewAccessLogProducer(brokers []string) sarama.AsyncProducer {
	// For the access log, we are looking for AP semantics, with high throughput.
	// By creating batches of compressed messages, we reduce network I/O at a cost of more latency.
	c := sarama.NewConfig()
	c.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	c.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	c.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	producer, err := sarama.NewAsyncProducer(brokers, c)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	// We will just log to STDOUT if we're not able to produce messages.
	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write access log entry:", err)
		}
	}()

	return producer
}
