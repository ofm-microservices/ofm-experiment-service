package projectionaudit

import "context"

// EntityAudit is the persisted parity evidence for one bounded context.
type EntityAudit struct {
	MicroserviceCount int64 `json:"microservice_count"`
	MonolithCount     int64 `json:"monolith_count"`
	MappingCount      int64 `json:"mapping_count"`
	OutboxPublished   int64 `json:"outbox_published"`
	OutboxPending     int64 `json:"outbox_pending"`
	Mismatches        int64 `json:"mismatches"`
	SourceEvents      int64 `json:"source_events"`
}

// Result contains all projection and fallback evidence collected for a run.
type Result struct {
	Entities map[string]EntityAudit `json:"entities"`
	Pass     bool                   `json:"pass"`
	Errors   []string               `json:"errors,omitempty"`
}

// Audit checks source records, outbox state, mappings and monolith projections.
type Auditor interface {
	Audit(ctx context.Context, started, finished string) (Result, error)
}
