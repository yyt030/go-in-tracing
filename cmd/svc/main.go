package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"

	"opentracing-zipkin-demo/config"
	"opentracing-zipkin-demo/monitor"

	"github.com/opentracing/opentracing-go"
)

var (
	cf      = flag.String("conf", "config.json", "config file name")
	addr    = flag.String("addr", ":8080", "listening for requests on")
	backend = flag.String("backend", "", "backend endpoint service. if exist must prefix with schema. example: http://localhost:9999")
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

	if *backend != "" {
		conf.RequestURL = *backend
	}

	if err = conf.VerifyConfig(); err != nil {
		flag.PrintDefaults()
		log.Fatal(err)
	}

	ms := NewMockServer(*conf, *addr)

	defer func() {
		if err := ms.Close(); err != nil {
			log.Println("Failed to close server", err)
		}
	}()

	log.Printf("Kafka brokers: %s", conf.BrokerList)

	log.Fatal(Run(ms))
}

func NewMockServer(c config.Conf, addr string) *monitor.MockServer {
	return &monitor.MockServer{
		Conf:              c,
		AccessLogProducer: monitor.NewAccessLogProducer(c.BrokerList),
		Server: http.Server{
			Addr:    addr,
			Handler: nil,
		},
		Tracer: monitor.NewTracer(c.ZipkinEndPoint, addr),
	}
}

func Run(ms *monitor.MockServer) error {
	ms.Server.Handler = ms.WithAccessLog()

	// Set tracer into default tracer
	opentracing.InitGlobalTracer(ms.Tracer)

	//server
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = ms.Conf.MaxIdleConnsPerHost

	log.Printf("Listening for requests on %s...\n", ms.Server.Addr)
	if ms.Conf.Profile {
		go func() {
			log.Println("profile listening on :8888")
			http.ListenAndServe("localhost:8888", nil)
		}()
	}
	return ms.Server.ListenAndServe()
}
