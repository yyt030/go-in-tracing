package monitor

import (
	"log"
	"net/http"

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
