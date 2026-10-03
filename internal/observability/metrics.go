package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"time"
)

var requests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ofm_load_test_service_requests_total", Help: "HTTP requests handled by load-test-service."}, []string{"method", "route", "status_class"})
var duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "ofm_load_test_service_request_duration_seconds", Help: "HTTP request duration."}, []string{"method", "route"})

func init() { prometheus.MustRegister(requests, duration) }
func Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(rw, r)
		requests.WithLabelValues(r.Method, r.URL.Path, statusClass(rw.status)).Inc()
		duration.WithLabelValues(r.Method, r.URL.Path).Observe(time.Since(started).Seconds())
	})
}
func MetricsHandler() http.Handler { return promhttp.Handler() }

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func statusClass(status int) string {
	if status >= 500 {
		return "5xx"
	}
	if status >= 400 {
		return "4xx"
	}
	if status >= 300 {
		return "3xx"
	}
	return "2xx"
}
