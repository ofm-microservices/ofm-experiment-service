package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ofm-microservices/ofm-experiment-service/internal/application"
	web "github.com/ofm-microservices/ofm-experiment-service/internal/presentation/http"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
)

func TestHTTP(t *testing.T) { RegisterFailHandler(Fail); RunSpecs(t, "Load Test HTTP") }

type runner struct{}

func (runner) Start(context.Context, application.StartInput) (application.Job, error) {
	return application.Job{Name: "job-1", RunID: "run-1", State: "PENDING"}, nil
}
func (runner) List(context.Context) ([]application.Job, error) {
	return []application.Job{{Name: "job-1", RunID: "run-1", State: "RUNNING"}}, nil
}
func (runner) Get(context.Context, string) (application.Job, error) {
	return application.Job{Name: "job-1", RunID: "run-1", State: "PASS"}, nil
}
func (runner) Stop(context.Context, string) error { return nil }

var _ = Describe("HTTP boundary", func() {
	var handler http.Handler
	BeforeEach(func() {
		svc, err := application.New(runner{}, zap.NewNop())
		Expect(err).NotTo(HaveOccurred())
		handler = web.New(svc, zap.NewNop())
	})

	It("serves health and metrics endpoints", func() {
		for _, path := range []string{"/healthz", "/metrics"} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			res := httptest.NewRecorder()
			handler.ServeHTTP(res, req)
			Expect(res.Code).To(Equal(http.StatusOK))
		}
	})

	It("starts a job with an HTML redirect", func() {
		req := httptest.NewRequest(http.MethodPost, "/runs", strings.NewReader("profile=smoke&vus=1&iterations=1"))
		req.Header.Set("Accept", "text/html")
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		Expect(res.Code).To(Equal(http.StatusSeeOther))
		Expect(res.Header().Get("Location")).To(Equal("/runs/job-1"))
	})
})
