package projectionaudit

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type entitySpec struct {
	name, sourceDB, sourceTable, sourceIDColumn, monolithTable, outboxType string
	mappingType, projectionMappingType, mappingColumn                      string
	sourceFilter, projectionFilter, outboxFilter                           string
	projectionDistinct, projectionDirect, hasOutbox                        bool
}

var sqlEntities = []entitySpec{
	{name: "user", sourceDB: "user_service", sourceTable: "users", sourceIDColumn: "user_id", monolithTable: "users", mappingType: "user", mappingColumn: "user_id", outboxType: "user", projectionDistinct: true, hasOutbox: true},
	{name: "auth_credentials", sourceDB: "auth_service", sourceTable: "auth_credentials", sourceIDColumn: "user_id", monolithTable: "auth_credentials", mappingType: "user", mappingColumn: "user_id", outboxType: "auth", outboxFilter: "payload ? 'email'", projectionDistinct: true, hasOutbox: true},
	{name: "registration", sourceDB: "registration_saga", sourceTable: "registration_saga_sessions", sourceIDColumn: "session_id", monolithTable: "registration_saga_sessions", mappingType: "registration", mappingColumn: "session_id", outboxType: "registration", projectionDistinct: true, hasOutbox: true},
	{name: "gig", sourceDB: "gig_service", sourceTable: "gigs", sourceIDColumn: "gig_id", monolithTable: "services", mappingType: "gig", mappingColumn: "gig_id", outboxType: "gigs", projectionDistinct: true, hasOutbox: true},
	{name: "gig_package", sourceDB: "gig_service", sourceTable: "gig_packages", sourceIDColumn: "package_id", monolithTable: "packages", mappingType: "package", mappingColumn: "package_id", outboxType: "gig_packages", projectionDistinct: true, hasOutbox: true},
	{name: "gig_question", sourceDB: "gig_service", sourceTable: "gig_questions", sourceIDColumn: "question_id", monolithTable: "service_questions", mappingType: "question", mappingColumn: "question_id", outboxType: "gig_questions", projectionDistinct: true, hasOutbox: true},
	{name: "gig_media", sourceDB: "gig_service", sourceTable: "gig_media", sourceIDColumn: "media_id", monolithTable: "services_files", mappingType: "media", mappingColumn: "media_id", outboxType: "gig_media", projectionDistinct: true, hasOutbox: true},
	{name: "file", sourceDB: "file_service", sourceTable: "files", sourceIDColumn: "file_id", monolithTable: "files", mappingType: "file", mappingColumn: "file_id", outboxType: "file", projectionDistinct: true, hasOutbox: true},
	{name: "order", sourceDB: "order_service", sourceTable: "orders", sourceIDColumn: "order_id", monolithTable: "orders", mappingType: "order", mappingColumn: "order_id", outboxType: "orders", projectionDistinct: true, hasOutbox: true},
	{name: "order_gig_snapshot", sourceDB: "order_service", sourceTable: "order_gig_snapshot", sourceIDColumn: "order_id", monolithTable: "order_gig_snapshot", mappingType: "order", mappingColumn: "order_id", outboxType: "orders", outboxFilter: "payload ? 'gig_title'", projectionDistinct: false, hasOutbox: true},
	{name: "order_question_snapshot", sourceDB: "order_service", sourceTable: "order_question_snapshots", sourceIDColumn: "order_id", monolithTable: "order_question_snapshots", mappingType: "order", mappingColumn: "order_id", outboxType: "orders", outboxFilter: "payload ? 'question_id' AND payload ? 'text'", projectionDistinct: false, hasOutbox: true},
	{name: "order_requirement_answer", sourceDB: "order_service", sourceTable: "order_requirement_answers", sourceIDColumn: "order_id", monolithTable: "order_requirement_answers", mappingType: "order", mappingColumn: "order_id", outboxType: "orders", outboxFilter: "payload ? 'question_id' AND payload ? 'answer_value'", projectionDistinct: false, hasOutbox: true},
	{name: "order_checkout_session", sourceDB: "order_service", sourceTable: "order_checkout_sessions", sourceIDColumn: "order_id", monolithTable: "order_checkout_sessions", mappingType: "order", mappingColumn: "order_id", outboxType: "orders", outboxFilter: "payload ? 'checkout_url'", projectionDistinct: false, hasOutbox: true},
	{name: "order_delivery", sourceDB: "order_service", sourceTable: "order_deliveries", sourceIDColumn: "order_id", monolithTable: "order_deliveries", mappingType: "order", mappingColumn: "order_id", outboxType: "orders", outboxFilter: "payload ? 'delivery_message'", projectionDistinct: false, hasOutbox: true},
	{name: "order_delivery_file", sourceDB: "order_service", sourceTable: "order_delivery_files", sourceIDColumn: "order_id", monolithTable: "order_delivery_files", mappingType: "order", mappingColumn: "order_id", outboxType: "orders", outboxFilter: "payload ? 'file_id' AND NOT (payload ? 'attachment_id')", projectionDistinct: false, hasOutbox: true},
	{name: "order_attachment", sourceDB: "order_service", sourceTable: "order_attachments", sourceIDColumn: "order_id", monolithTable: "order_attachments", mappingType: "order", mappingColumn: "order_id", outboxType: "orders", outboxFilter: "payload ? 'attachment_id'", projectionDistinct: false, hasOutbox: true},
	{name: "order_revision_request", sourceDB: "order_service", sourceTable: "order_revision_requests", sourceIDColumn: "order_id", monolithTable: "order_revision_requests", mappingType: "order", mappingColumn: "order_id", outboxType: "orders", outboxFilter: "payload ? 'reason' AND payload ? 'buyer_id' AND NOT (payload ? 'dispute_type')", projectionDistinct: false, hasOutbox: true},
	{name: "order_dispute", sourceDB: "order_service", sourceTable: "order_disputes", sourceIDColumn: "order_id", monolithTable: "order_disputes", mappingType: "order", mappingColumn: "order_id", outboxType: "orders", outboxFilter: "payload ? 'dispute_type'", projectionDistinct: false, hasOutbox: true},
	// Legacy chats are keyed by order ID, while messages have their own mapping.
	{name: "chat", sourceDB: "chat_service", sourceTable: "chats_by_order", sourceIDColumn: "order_id", monolithTable: "chats_by_order", mappingType: "chat", projectionMappingType: "order", mappingColumn: "order_id", outboxType: "chat", outboxFilter: "NOT (payload ? 'message_id')", projectionDistinct: true, hasOutbox: true},
	{name: "chat_message", sourceDB: "chat_service", sourceTable: "chat_messages", sourceIDColumn: "message_id", monolithTable: "chat_messages_by_order", mappingType: "message", mappingColumn: "message_id", outboxType: "chat", outboxFilter: "payload ? 'message_id'", projectionDistinct: true, hasOutbox: true},
	{name: "review", sourceDB: "review_service", sourceTable: "reviews", sourceIDColumn: "review_id", monolithTable: "reviews", mappingType: "review", mappingColumn: "review_id", outboxType: "reviews", projectionDistinct: true, hasOutbox: true},
	{name: "payment_intent", sourceDB: "payment_service", sourceTable: "payment_intents", sourceIDColumn: "payment_intent_id", monolithTable: "payment_intents", mappingType: "payment_intent", mappingColumn: "payment_intent_id", outboxType: "payment_intents", projectionDistinct: true, hasOutbox: true},
	// Webhook event IDs are provider strings, not UUID-backed legacy IDs. Audit
	// them directly by event_id instead of forcing a text-to-bigint join.
	{name: "payment_webhook_event", sourceDB: "payment_service", sourceTable: "payment_webhook_events", sourceIDColumn: "event_id", monolithTable: "payment_webhook_events", mappingColumn: "event_id", outboxType: "payment_webhook_events", projectionDirect: true, projectionDistinct: true, hasOutbox: true},
	{name: "connect_account", sourceDB: "payment_service", sourceTable: "connect_accounts", sourceIDColumn: "user_id", monolithTable: "connect_accounts", mappingType: "user", mappingColumn: "user_id", outboxType: "connect_accounts", projectionDistinct: true, hasOutbox: true},
	{name: "payment_release", sourceDB: "payment_service", sourceTable: "payment_releases", sourceIDColumn: "payment_release_id", monolithTable: "payment_releases", mappingType: "payment_release", mappingColumn: "payment_release_id", outboxType: "payment_releases", projectionDistinct: true, hasOutbox: true},
	{name: "payment_transfer", sourceDB: "payment_service", sourceTable: "payment_releases", sourceIDColumn: "payment_release_id", monolithTable: "payment_releases", mappingType: "payment_release", mappingColumn: "payment_release_id", sourceFilter: "stripe_transfer_id <> ''", projectionFilter: "t.stripe_transfer_id <> ''", outboxType: "payment_releases", outboxFilter: "payload->>'stripe_transfer_id' <> ''", projectionDistinct: true, hasOutbox: true},
	{name: "payment_refund", sourceDB: "payment_service", sourceTable: "payment_releases", sourceIDColumn: "payment_release_id", monolithTable: "payment_releases", mappingType: "payment_release", mappingColumn: "payment_release_id", sourceFilter: "stripe_refund_id <> ''", projectionFilter: "t.stripe_refund_id <> ''", outboxType: "payment_releases", outboxFilter: "payload->>'stripe_refund_id' <> ''", projectionDistinct: true, hasOutbox: true},
	{name: "order_saga", sourceDB: "order_saga", sourceTable: "order_saga_sessions", sourceIDColumn: "saga_id", monolithTable: "order_saga_sessions", mappingType: "saga", mappingColumn: "saga_id", outboxType: "saga", projectionDistinct: true, hasOutbox: true},
	// Registration currently has one persisted saga session table; expose both
	// requested business and saga labels while keeping their event scope exact.
	{name: "registration_saga", sourceDB: "registration_saga", sourceTable: "registration_saga_sessions", sourceIDColumn: "session_id", monolithTable: "registration_saga_sessions", mappingType: "registration", mappingColumn: "session_id", outboxType: "registration", projectionDistinct: true, hasOutbox: true},
}

type sqlAuditor struct {
	monolithURL string
	serviceURLs map[string]string
}

// NewSQL creates an auditor for the SQL-backed bounded contexts. URLs are read
// from OFM_AUDIT_MONOLITH_URL and OFM_AUDIT_<ENTITY>_URL variables.
func NewSQL() Auditor {
	if os.Getenv("OFM_AUDIT_MONOLITH_URL") == "" {
		return nil
	}
	services := make(map[string]string, len(sqlEntities))
	for _, spec := range sqlEntities {
		services[spec.name] = os.Getenv("OFM_AUDIT_" + auditServiceEnvName(spec.sourceDB) + "_URL")
	}
	return &sqlAuditor{monolithURL: os.Getenv("OFM_AUDIT_MONOLITH_URL"), serviceURLs: services}
}

// auditServiceEnvName maps an owning database name to the stable audit
// environment variable used by the runner deployment. Database names are
// intentionally more specific than the service labels exposed in the
// deployment, for example user_service is configured as OFM_AUDIT_USER_URL.
func auditServiceEnvName(sourceDB string) string {
	aliases := map[string]string{
		"user_service":      "USER",
		"auth_service":      "AUTH",
		"registration_saga": "REGISTRATION",
		"gig_service":       "GIG",
		"file_service":      "FILE",
		"order_service":     "ORDER",
		"chat_service":      "CHAT",
		"review_service":    "REVIEW",
		"payment_service":   "PAYMENT",
		"order_saga":        "ORDER_SAGA",
	}
	if name, ok := aliases[sourceDB]; ok {
		return name
	}
	return strings.ToUpper(sourceDB)
}

func (a *sqlAuditor) Audit(ctx context.Context, started, finished string) (Result, error) {
	result := Result{Entities: make(map[string]EntityAudit, len(sqlEntities)), Pass: true}
	if a.monolithURL == "" {
		return result, fmt.Errorf("projection audit monolith URL is not configured")
	}
	monolith, err := sql.Open("pgx", a.monolithURL)
	if err != nil {
		return result, fmt.Errorf("open monolith audit DB: %w", err)
	}
	defer monolith.Close()
	if err = monolith.PingContext(ctx); err != nil {
		return result, fmt.Errorf("ping monolith audit DB: %w", err)
	}
	for _, spec := range sqlEntities {
		item, itemErr := a.auditEntity(ctx, monolith, spec, started, finished)
		if itemErr != nil {
			result.Errors = append(result.Errors, spec.name+": "+itemErr.Error())
			result.Pass = false
			continue
		}
		result.Entities[spec.name] = item
		if item.Mismatches != 0 || item.OutboxPending != 0 {
			result.Pass = false
		}
	}
	return result, nil
}

func (a *sqlAuditor) auditEntity(ctx context.Context, monolith *sql.DB, spec entitySpec, started, finished string) (EntityAudit, error) {
	var result EntityAudit
	dsn := a.serviceURLs[spec.name]
	if dsn == "" {
		return result, fmt.Errorf("service URL is not configured")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return result, fmt.Errorf("open service DB: %w", err)
	}
	defer db.Close()
	if err = db.PingContext(ctx); err != nil {
		return result, fmt.Errorf("ping service DB: %w", err)
	}
	sourceQuery := "select count(*) from " + spec.sourceTable + " where created_at >= $1 and created_at <= $2"
	if spec.sourceFilter != "" {
		sourceQuery += " and " + spec.sourceFilter
	}
	if err = db.QueryRowContext(ctx, sourceQuery, started, finished).Scan(&result.MicroserviceCount); err != nil {
		return result, fmt.Errorf("count source records: %w", err)
	}
	if spec.hasOutbox {
		// Outbox rows use occurred_at as their event timestamp. The business
		// tables use created_at, but that column is not part of the generic
		// outbox contract and does not exist on outbox_events.
		outboxQuery := "select count(*) from outbox_events where occurred_at >= $1 and occurred_at <= $2"
		outboxArgs := []any{started, finished}
		if spec.outboxType != "" {
			outboxQuery += " and aggregate_type=$3"
			outboxArgs = append(outboxArgs, spec.outboxType)
		}
		if spec.outboxFilter != "" {
			outboxQuery += " and " + spec.outboxFilter
		}
		if err = db.QueryRowContext(ctx, outboxQuery, outboxArgs...).Scan(&result.OutboxPublished); err != nil {
			return result, fmt.Errorf("count outbox records: %w", err)
		}
	}
	// Service outbox rows do not carry a mutable status; all rows are CDC input.
	// pending is therefore derived from the projection gap, not guessed as zero.
	// Projection upserts update the legacy row's timestamps, so filtering the
	// monolith table by created_at counts historical rows touched by this run.
	// The migration identity mapping is the stable run-scoped boundary: count
	// only legacy rows whose UUID mapping was created during this experiment.
	sourceIDs, sourceIDErr := sourceIDs(ctx, db, spec, started, finished)
	if sourceIDErr != nil {
		return result, sourceIDErr
	}
	if len(sourceIDs) == 0 {
		return result, nil
	}
	ids := "{" + strings.Join(sourceIDs, ",") + "}"
	if spec.projectionDirect {
		monolithQuery := "select count(distinct t." + spec.mappingColumn + ") from " + spec.monolithTable + " t where t." + spec.mappingColumn + " = any($1::text[])"
		if err = monolith.QueryRowContext(ctx, monolithQuery, ids).Scan(&result.MonolithCount); err != nil {
			return result, fmt.Errorf("count monolith projection: %w", err)
		}
	} else {
		projectionType := projectionMappingType(spec)
		projectionCount := "count(*)"
		if spec.projectionDistinct {
			projectionCount = "count(distinct m.uuid_id)"
		}
		monolithQuery := "select " + projectionCount + " from migration_id_mapping m join " + spec.monolithTable + " t on t." + spec.mappingColumn + " = m.legacy_id where m.entity_type=$1 and m.uuid_id = any($2::uuid[])"
		if spec.projectionFilter != "" {
			monolithQuery += " and " + spec.projectionFilter
		}
		if err = monolith.QueryRowContext(ctx, monolithQuery, projectionType, ids).Scan(&result.MonolithCount); err != nil {
			return result, fmt.Errorf("count monolith projection: %w", err)
		}
	}
	// Count mappings for the audited monolith rows, not every auxiliary
	// identity created while processing the same workflow (for example,
	// registration clients and order-saga steps).
	if spec.mappingType != "" {
		mappingQuery := "select count(distinct uuid_id) from migration_id_mapping where entity_type=$1 and uuid_id = any($2::uuid[])"
		if err = monolith.QueryRowContext(ctx, mappingQuery, spec.mappingType, ids).Scan(&result.MappingCount); err != nil {
			return result, fmt.Errorf("count identity mappings: %w", err)
		}
	}
	result.Mismatches = result.MicroserviceCount - result.MonolithCount
	if result.Mismatches < 0 {
		result.Mismatches = -result.Mismatches
	}
	if result.MicroserviceCount > result.MonolithCount {
		result.OutboxPending = result.MicroserviceCount - result.MonolithCount
	}
	return result, nil
}

func projectionMappingType(spec entitySpec) string {
	if spec.projectionMappingType != "" {
		return spec.projectionMappingType
	}
	return spec.mappingType
}

func sourceIDs(ctx context.Context, db *sql.DB, spec entitySpec, started, finished string) ([]string, error) {
	query := "select distinct " + spec.sourceIDColumn + " from " + spec.sourceTable + " where created_at >= $1 and created_at <= $2"
	if spec.sourceFilter != "" {
		query += " and " + spec.sourceFilter
	}
	rows, err := db.QueryContext(ctx, query, started, finished)
	if err != nil {
		return nil, fmt.Errorf("list source identities: %w", err)
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan source identity: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read source identities: %w", err)
	}
	return ids, nil
}

func serviceURL(host string, port int, db string) string {
	return "postgres://admin:admin@" + host + ":" + strconv.Itoa(port) + "/" + url.PathEscape(db) + "?sslmode=disable"
}
