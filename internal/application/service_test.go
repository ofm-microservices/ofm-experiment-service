package application_test

import (
	"context"
	"github.com/ofm-microservices/ofm-experiment-service/internal/application"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"testing"
)

func TestApplication(t *testing.T) { RegisterFailHandler(Fail); RunSpecs(t, "Load Test Application") }

type runner struct{}

func (runner) Start(context.Context, application.StartInput) (application.Job, error) {
	return application.Job{Name: "job-1", RunID: "run-1", State: "PENDING"}, nil
}
func (runner) List(context.Context) ([]application.Job, error) {
	return []application.Job{{Name: "job-1", State: "RUNNING"}}, nil
}
func (runner) Get(context.Context, string) (application.Job, error) {
	return application.Job{Name: "job-1"}, nil
}
func (runner) Stop(context.Context, string) error { return nil }

var _ = Describe("Service", func() {
	It("delegates job lifecycle to the boundary", func() {
		svc, err := application.New(runner{}, nil)
		Expect(err).NotTo(HaveOccurred())
		job, err := svc.Start(context.Background(), application.StartInput{VUs: 1})
		Expect(err).NotTo(HaveOccurred())
		Expect(job.Name).To(Equal("job-1"))
		jobs, err := svc.List(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(jobs).To(HaveLen(1))
		Expect(svc.Stop(context.Background(), "job-1")).To(Succeed())
	})
	It("rejects a nil runner", func() { _, err := application.New(nil, nil); Expect(err).To(MatchError(application.ErrNilJobRunner)) })
})
