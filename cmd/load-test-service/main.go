package main

import (
	"context"
	"github.com/ofm-microservices/ofm-experiment-service/internal/application"
	"github.com/ofm-microservices/ofm-experiment-service/internal/infra/kubernetes"
	web "github.com/ofm-microservices/ofm-experiment-service/internal/presentation/http"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer log.Sync()
	runner := kubernetes.NewRunner(os.Getenv("RUNNER_NAMESPACE"), os.Getenv("K6_IMAGE"), log)
	svc, err := application.New(runner, log)
	if err != nil {
		log.Fatal("application construction failed", zap.Error(err))
	}
	provider := tracerProvider(log)
	defer func() { _ = provider.Shutdown(context.Background()) }()
	server := &http.Server{Addr: ":" + env("PORT", "8080"), Handler: otelhttp.NewHandler(web.New(svc, log), "http.load-test-service")}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("http server failed", zap.Error(err))
		}
	}()
	log.Info("load test service started", zap.String("service", "load-test-service"), zap.String("operation", "startup"))
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}
func tracerProvider(log *zap.Logger) *trace.TracerProvider {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://otel-collector:4318"
	}
	exporter, err := otlptracehttp.New(context.Background(), otlptracehttp.WithEndpoint(strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")), otlptracehttp.WithInsecure())
	if err != nil {
		log.Warn("otel exporter disabled", zap.Error(err))
		return trace.NewTracerProvider()
	}
	tp := trace.NewTracerProvider(trace.WithBatcher(exporter))
	otel.SetTracerProvider(tp)
	return tp
}
func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
