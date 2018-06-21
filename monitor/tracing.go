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

	"github.com/opentracing/opentracing-go"
	"github.com/openzipkin/zipkin-go-opentracing"
)

func NewTracer(zkAddr string, svc string) opentracing.Tracer {
	// Create collector
	collector, err := zipkintracer.NewHTTPCollector(zkAddr)
	if err != nil {
		log.Fatalf("create zipkin collector failed:%v", err)
	}

	// Create recorder and define service
	recorder := zipkintracer.NewRecorder(collector, false, zkAddr, svc)

	// Create tracer
	tracer, err := zipkintracer.NewTracer(recorder, zipkintracer.ClientServerSameSpan(true), zipkintracer.TraceID128Bit(true))
	if err != nil {
		log.Fatalf("create tracer failed:%v", err)
	}

	return tracer
}

func NewSpan(r *http.Request) opentracing.Span {
	var sp opentracing.Span
	opName := r.URL.Path
	ctx, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, opentracing.HTTPHeadersCarrier(r.Header))
	if err != nil {
		// If for whatever reason we can't join, start a new root span
		sp = opentracing.StartSpan(opName)
	} else {
		sp = opentracing.StartSpan(opName, opentracing.ChildOf(ctx))
	}
	return sp
}

func NewCliReq(c config.Conf, sp opentracing.Span) ([]byte, error) {
	// Create new request via http
	tr := &http.Transport{
		MaxIdleConnsPerHost: c.MaxIdleConnsPerHost,
		IdleConnTimeout:     30 * time.Second,
	}

	req, err := http.NewRequest("GET", c.RequestURL, nil)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("create new request failed:%v", err))
	}
	// Inject the sp into request header
	if err := sp.Tracer().Inject(sp.Context(), opentracing.TextMap, opentracing.HTTPHeadersCarrier(req.Header)); err != nil {
		return nil, errors.New(fmt.Sprintf("sp inject error: %v", err))
	}

	http.DefaultClient.Transport = tr
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return nil, errors.New(fmt.Sprintf("request failed: %v", err))
	}
	defer resp.Body.Close()

	io.Copy(ioutil.Discard, resp.Body)

	return nil, err
}
