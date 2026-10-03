package application

import (
	"context"
	"errors"
	"go.uber.org/zap"
)

var ErrNilJobRunner = errors.New("job runner is nil")
var ErrNilRunReader = errors.New("run reader is nil")

// Service orchestrates load-test commands without owning Kubernetes details.
type Service struct {
	runner JobRunner
	reader RunReader
	log    *zap.Logger
}

// New constructs the application boundary for load-test control.
func New(runner JobRunner, log *zap.Logger) (*Service, error) {
	if runner == nil {
		return nil, ErrNilJobRunner
	}
	reader, _ := runner.(RunReader)
	return &Service{runner: runner, reader: reader, log: log}, nil
}
func (s *Service) Start(ctx context.Context, input StartInput) (Job, error) {
	return s.runner.Start(ctx, input)
}
func (s *Service) List(ctx context.Context) ([]Job, error)           { return s.runner.List(ctx) }
func (s *Service) Get(ctx context.Context, name string) (Job, error) { return s.runner.Get(ctx, name) }
func (s *Service) Stop(ctx context.Context, name string) error       { return s.runner.Stop(ctx, name) }

// ListRuns returns one page of durable ClickHouse records filtered by an optional run ID.
func (s *Service) ListRuns(ctx context.Context, search string, page int, pageSize int) ([]Run, error) {
	if s.reader == nil {
		return nil, ErrNilRunReader
	}
	return s.reader.ListRuns(ctx, search, page, pageSize)
}

// GetRun returns one durable ClickHouse record by run ID.
func (s *Service) GetRun(ctx context.Context, runID string) (Run, error) {
	if s.reader == nil {
		return Run{}, ErrNilRunReader
	}
	return s.reader.GetRun(ctx, runID)
}
