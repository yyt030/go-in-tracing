package monitor

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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
	Client            *http.Client
}

func (ms *MockServer) Close() error {
	if err := ms.AccessLogProducer.Close(); err != nil {
		log.Println("Failed to shut down access log producer cleanly", err)
	}

	return nil
}

func (ms *MockServer) NewCliReq(sp opentracing.Span) ([]byte, error) {
	// Create new request via http
	req, err := http.NewRequest("GET", ms.Conf.RequestURL, nil)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("create new request failed:%v", err))
	}
	// Inject the sp into request header
	if err := sp.Tracer().Inject(sp.Context(), opentracing.TextMap, opentracing.HTTPHeadersCarrier(req.Header)); err != nil {
		return nil, errors.New(fmt.Sprintf("sp inject error: %v", err))
	}

	//resp, err := http.DefaultClient.Do(req)
	resp, err := ms.Client.Do(req)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("request failed: %v", err))
	}
	defer resp.Body.Close()

	io.Copy(ioutil.Discard, resp.Body)

	return nil, err
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
		sp.LogKV("client_send", "test")
		if ms.Conf.RequestURL != "" {
			_, err := ms.NewCliReq(sp)
			if err != nil {
				log.Println("request service1 failed:", err)
			}
		}
		sp.LogKV("client_receive", "test")

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

func NewAccessLogProducer(brokers []string, n int) sarama.AsyncProducer {
	// For the access log, we are looking for AP semantics, with high throughput.
	// By creating batches of compressed messages, we reduce network I/O at a cost of more latency.
	c := sarama.NewConfig()
	c.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	c.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	c.Producer.Flush.Frequency = time.Duration(n) * time.Millisecond // Flush batches every 500ms

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
