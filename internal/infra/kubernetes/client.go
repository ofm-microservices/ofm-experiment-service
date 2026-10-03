package kubernetes

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ofm-microservices/ofm-experiment-service/internal/application"
	"github.com/ofm-microservices/ofm-experiment-service/internal/infra/projectionaudit"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type client struct {
	namespace, image, api, token, ca, clickhouse string
	clickhouseUser, clickhousePassword           string
	kafkaBrokers                                 string
	auditor                                      projectionaudit.Auditor
	http                                         *http.Client
	log                                          *zap.Logger
	persistMu                                    sync.Mutex
	persisting                                   map[string]struct{}
}

var errRecoveryEvidencePending = errors.New("recovery evidence pending")
var errRunNotFound = errors.New("experiment run not found")

type deploymentObject struct {
	Spec struct {
		Replicas int `json:"replicas"`
	} `json:"spec"`
}

type horizontalPodAutoscalerObject struct {
	Spec struct {
		MinReplicas *int `json:"minReplicas"`
	} `json:"spec"`
}

// NewRunner creates a service-account Kubernetes adapter for Job lifecycle operations.
func NewRunner(namespace, image string, log *zap.Logger) application.JobRunner {
	if namespace == "" {
		namespace = "ofm"
	}
	if image == "" {
		image = "ofm/k6-full-system:latest"
	}
	pool, _ := x509.SystemCertPool()
	if pool == nil {
		pool = x509.NewCertPool()
	}
	if cert, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"); err == nil {
		pool.AppendCertsFromPEM(cert)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}}
	return &client{namespace: namespace, image: image, clickhouse: os.Getenv("CLICKHOUSE_URL"), clickhouseUser: os.Getenv("CLICKHOUSE_USER"), clickhousePassword: os.Getenv("CLICKHOUSE_PASSWORD"), kafkaBrokers: strings.TrimSpace(os.Getenv("KAFKA_BROKERS")), auditor: projectionaudit.NewSQL(), api: "https://" + os.Getenv("KUBERNETES_SERVICE_HOST") + ":" + os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS"), token: read("/var/run/secrets/kubernetes.io/serviceaccount/token"), ca: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt", http: &http.Client{Transport: transport, Timeout: 15 * time.Second}, log: log, persisting: make(map[string]struct{})}
}

// ListRuns reads durable experiment results from ClickHouse. Kubernetes jobs
// are intentionally not used here because their retention is shorter than the
// experiment history.
func (c *client) ListRuns(ctx context.Context, search string, page int, pageSize int) ([]application.Run, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	filter := ""
	if value := strings.TrimSpace(search); value != "" {
		filter = " WHERE run_id ILIKE '%" + strings.ReplaceAll(value, "'", "''") + "%'"
	}
	offset := (page - 1) * pageSize
	query := fmt.Sprintf("SELECT run_id, toString(started_at) AS started_at, toString(finished_at) AS finished_at, status, verdict, checks_failed, projection_failures, projection_pending, JSONExtractInt(result_json, 'full_system_completed_flows') AS completed_flows, p95_duration_ms FROM ofm_load_test_runs%s ORDER BY started_at DESC LIMIT %d OFFSET %d FORMAT JSONEachRow", filter, pageSize+1, offset)
	return c.queryRuns(ctx, query)
}

// GetRun reads one durable experiment result by its run identifier.
func (c *client) GetRun(ctx context.Context, runID string) (application.Run, error) {
	value := strings.ReplaceAll(strings.TrimSpace(runID), "'", "''")
	query := "SELECT run_id, toString(started_at) AS started_at, toString(finished_at) AS finished_at, status, verdict, checks_failed, projection_failures, projection_pending, JSONExtractInt(result_json, 'full_system_completed_flows') AS completed_flows, p95_duration_ms FROM ofm_load_test_runs WHERE run_id='" + value + "' ORDER BY started_at DESC LIMIT 1 FORMAT JSONEachRow"
	runs, err := c.queryRuns(ctx, query)
	if err != nil {
		return application.Run{}, err
	}
	if len(runs) == 0 {
		return application.Run{}, errRunNotFound
	}
	return runs[0], nil
}

func (c *client) queryRuns(ctx context.Context, query string) ([]application.Run, error) {
	if c.clickhouse == "" {
		return nil, errors.New("ClickHouse is not configured")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.clickhouse+"/?query="+url.QueryEscape(query), nil)
	if err != nil {
		return nil, err
	}
	if c.clickhouseUser != "" {
		request.SetBasicAuth(c.clickhouseUser, c.clickhousePassword)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("ClickHouse query status %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	runs := make([]application.Run, 0)
	decoder := json.NewDecoder(response.Body)
	for {
		var run application.Run
		if err := decoder.Decode(&run); err != nil {
			if errors.Is(err, io.EOF) {
				return runs, nil
			}
			return nil, err
		}
		runs = append(runs, run)
	}
}
func read(path string) string { b, _ := os.ReadFile(path); return strings.TrimSpace(string(b)) }
func (c *client) request(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.api+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	if method == http.MethodPatch {
		req.Header.Set("Content-Type", "application/merge-patch+json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("kubernetes %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

type jobObject struct {
	Metadata struct {
		Name              string            `json:"name"`
		Labels            map[string]string `json:"labels"`
		Annotations       map[string]string `json:"annotations"`
		CreationTimestamp string            `json:"creationTimestamp"`
	} `json:"metadata"`
	Status struct {
		Active     int `json:"active"`
		Succeeded  int `json:"succeeded"`
		Failed     int `json:"failed"`
		Conditions []struct {
			Type    string `json:"type"`
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"conditions"`
	} `json:"status"`
}

func (c *client) Start(ctx context.Context, in application.StartInput) (application.Job, error) {
	// Do not start a run while the shared Kafka broker is still recovering after
	// a host/k3d restart. A TCP listener alone is insufficient here: Kafka may
	// accept connections while group-coordinator metadata is not ready yet.
	// Starting k6 in that window creates users and then loses the saga result
	// path, producing a misleading BackoffLimitExceeded job.
	if err := c.waitForKafkaReady(ctx); err != nil {
		return application.Job{}, fmt.Errorf("kafka is not ready for experiment: %w", err)
	}
	if in.ReadRPS+in.WriteRPS > 0 {
		in.TargetRPS = in.ReadRPS + in.WriteRPS
	}
	if strings.EqualFold(in.Workload, "full-system") && in.TargetRPS > 0 {
		// The UI values are request rates. Pass their sum directly to k6;
		// never convert it into a guessed number of full business flows.
		in.Rate = in.TargetRPS
	}
	// The full-system k6 scenario is arrival-rate based. Keep Rate as the
	// canonical value sent to k6; otherwise a UI request containing only
	// read_rps/write_rps leaves OFM_FULL_K6_RATE at zero and silently falls
	// back to constant-vus.
	if in.Rate < 1 && in.TargetRPS > 0 && !strings.EqualFold(in.Workload, "full-system") {
		in.Rate = in.TargetRPS
	}
	if in.Rate > 0 {
		// Do not derive concurrency from arrival rate when the caller already
		// supplied it. A full-system iteration is a long workflow (registration,
		// projections, order, payment, chat and review); one flow per second can
		// legitimately require dozens of concurrent VUs. The old rate*2/rate*3
		// heuristic reduced rate=1 to maxVUs=4 and starved the workflow.
		if in.PreAllocatedVUs < 1 {
			in.PreAllocatedVUs = in.Rate * 2
			if in.PreAllocatedVUs < 2 {
				in.PreAllocatedVUs = 2
			}
		}
	}
	run := newRunID()
	name := run
	if in.VUs < 1 {
		in.VUs = 1
	}
	if in.PreAllocatedVUs < 1 {
		in.PreAllocatedVUs = 200
	}
	if in.MaxVUs < 1 {
		return application.Job{}, errors.New("MAX_VUS must be configured or max_vus must be provided")
	}
	if in.Iterations < 1 {
		in.Iterations = 1
	}
	var faultRestore func()
	var faultAt time.Duration
	// Mixed injects the configured request-level fault rate.  Do not silently
	// turn it into a full service outage: that made the default 5% experiment
	// stop the target deployment for 30 seconds and caused false projection
	// timeouts.  A service outage is explicit through service-down or FaultAt.
	if (strings.EqualFold(in.FaultProfile, "service-down") || strings.TrimSpace(in.FaultAt) != "") && in.FaultTarget != "" && in.FaultTarget != "*" {
		if parsed, err := time.ParseDuration(in.FaultAt); err == nil && parsed > 0 {
			faultAt = parsed
		} else {
			restore, err := c.stopFaultTarget(ctx, in.FaultTarget)
			if err != nil {
				return application.Job{}, fmt.Errorf("start mixed fault target: %w", err)
			}
			faultRestore = restore
		}
	}
	createUsers := "true"
	if strings.EqualFold(strings.TrimSpace(in.FaultTarget), "chat-service") || strings.EqualFold(strings.TrimSpace(in.FaultTarget), "chat") {
		// Chat recovery needs a valid order/chat aggregate. Reuse the fixture
		// identity for targeted fault tests instead of creating a new, still
		// asynchronous registration/order graph in every VU.
		createUsers = "false"
	}
	obj := map[string]any{"apiVersion": "batch/v1", "kind": "Job", "metadata": map[string]any{"name": name, "namespace": c.namespace, "labels": map[string]string{"ofm.test.run_id": run}, "annotations": map[string]string{"ofm.experiment.duration_seconds": strconv.Itoa(durationSeconds(in.Duration)), "ofm.experiment.vus": strconv.Itoa(in.VUs), "ofm.experiment.rate": strconv.Itoa(in.TargetRPS), "ofm.experiment.target_rps": strconv.Itoa(in.TargetRPS), "ofm.experiment.profile": in.Profile}}, "spec": map[string]any{"backoffLimit": 0, "ttlSecondsAfterFinished": 3600, "template": map[string]any{"metadata": map[string]any{"labels": map[string]string{"ofm.test.run_id": run}}, "spec": map[string]any{"restartPolicy": "Never", "containers": []any{map[string]any{"name": "k6", "image": c.image, "imagePullPolicy": "IfNotPresent", "command": []string{"k6", "run", "/work/full-system.js"}, "env": []map[string]string{{"name": "K6_TEST_RUN_ID", "value": run}, {"name": "K6_SCENARIO", "value": "full-system-" + run}, {"name": "OFM_FULL_K6_VUS", "value": fmt.Sprint(in.VUs)}, {"name": "OFM_FULL_K6_ITERATIONS", "value": fmt.Sprint(in.Iterations)}, {"name": "OFM_FULL_K6_MAX_DURATION", "value": in.Duration}, {"name": "OFM_FULL_K6_DURATION", "value": in.Duration}, {"name": "OFM_FULL_K6_RATE", "value": fmt.Sprint(in.Rate)}, {"name": "OFM_FULL_K6_PRE_ALLOCATED_VUS", "value": fmt.Sprint(in.PreAllocatedVUs)}, {"name": "OFM_FULL_K6_MAX_VUS", "value": fmt.Sprint(in.MaxVUs)}, {"name": "FAULT_PROFILE", "value": in.FaultProfile}, {"name": "FAULT_TARGET", "value": in.FaultTarget}, {"name": "FAULT_RATE", "value": fmt.Sprint(in.FaultRate)}, {"name": "FAULT_DELAY", "value": in.FaultDelay}, {"name": "FAULT_MAX_FAILURES", "value": fmt.Sprint(in.FaultMaxFailures)}, {"name": "FAULT_TTL", "value": in.FaultTTL}, {"name": "OFM_K6_ACCEPT_5XX", "value": strconv.FormatBool(in.Accept5xx)}, {"name": "OFM_K6_ALLOW_SLOW", "value": strconv.FormatBool(in.AllowSlow)}, {"name": "K6_CREATE_USERS", "value": createUsers}, {"name": "K6_TEST_PASSWORD", "value": "Password123!"}, {"name": "K6_BASE_URL", "value": "http://api-gateway:8080/api/v2"}, {"name": "K6_PAYMENT_URL", "value": "http://payment-service:8081/v1"}}}}}}}}
	// Keep the pod label under metadata.labels. Kubernetes silently drops a
	// scalar metadata field here, which made completed k6 logs impossible to
	// find by run ID.
	obj["spec"].(map[string]any)["template"].(map[string]any)["metadata"] = map[string]any{"labels": map[string]string{"ofm.test.run_id": run}}
	templateSpec := obj["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	container := templateSpec["containers"].([]any)[0].(map[string]any)
	if strings.EqualFold(strings.TrimSpace(in.Workload), "full-system") {
		env := container["env"].([]map[string]string)
		// Stop request-generation scenarios at the configured load duration.
		// The extra window is reserved for passive observation and read-only
		// projection auditing.
		env = append(env,
			map[string]string{"name": "OFM_FULL_K6_GRACEFUL_STOP", "value": "0s"},
			map[string]string{"name": "OFM_FULL_K6_DRAIN_TIMEOUT", "value": "120s"},
		)
		container["env"] = env
	}
	container["env"] = append(container["env"].([]map[string]string), map[string]string{
		"name":  "K6_FIXTURE_REVIEW_ORDER_ID",
		"value": os.Getenv("K6_FIXTURE_REVIEW_ORDER_ID"),
	}, map[string]string{
		"name":  "K6_FIXTURE_BUYER_ID",
		"value": os.Getenv("K6_FIXTURE_BUYER_ID"),
	}, map[string]string{
		"name":  "K6_ORDER_ID",
		"value": os.Getenv("K6_ORDER_ID"),
	}, map[string]string{
		"name":  "K6_FIXTURE_ORDER_ID",
		"value": os.Getenv("K6_FIXTURE_ORDER_ID"),
	})
	workload := strings.ToLower(strings.TrimSpace(in.Workload))
	if strings.EqualFold(in.LoadMode, "pc-capacity") {
		container["image"] = valueOrEnv("CAPACITY_CONTROLLER_IMAGE", "ofm/load-test-service:capacity4")
		templateSpec["serviceAccountName"] = "load-test-service"
	}
	// The experiment service has one canonical E2E workload. It exercises the
	// complete HTTP journey; faultTarget selects the service to disrupt and
	// must not change the business flow to a service-specific probe.
	container["command"] = []string{"k6", "run", "/work/full-system.js"}
	workload = "full-system"
	env := container["env"].([]map[string]string)
	env = append(env,
		map[string]string{"name": "OFM_FULL_K6_READ_RPS", "value": fmt.Sprint(in.ReadRPS)},
		map[string]string{"name": "OFM_FULL_K6_WRITE_RPS", "value": fmt.Sprint(in.WriteRPS)},
		map[string]string{"name": "FAULT_AT", "value": in.FaultAt},
		map[string]string{"name": "FAULT_DURATION", "value": in.FaultDuration},
		map[string]string{"name": "K6_FIXTURE_USERNAME", "value": os.Getenv("K6_FIXTURE_USERNAME")},
		map[string]string{"name": "K6_FIXTURE_PASSWORD", "value": os.Getenv("K6_FIXTURE_PASSWORD")},
		map[string]string{"name": "K6_USER_READ_USERNAME", "value": os.Getenv("K6_USER_READ_USERNAME")},
		map[string]string{"name": "K6_FIXTURE_GIG_ID", "value": os.Getenv("K6_FIXTURE_GIG_ID")},
		map[string]string{"name": "K6_PACKAGE_ID", "value": os.Getenv("K6_PACKAGE_ID")},
	)
	if in.Profile == "capacity-ramp" {
		env = append(env, map[string]string{"name": "OFM_CAPACITY_RAMP", "value": "true"}, map[string]string{"name": "OFM_CAPACITY_RAMP_SECONDS", "value": "10s"})
	}
	if strings.EqualFold(in.LoadMode, "pc-capacity") {
		container["command"] = []string{"/capacity-controller"}
		env = append(env,
			map[string]string{"name": "EXPERIMENT_URL", "value": "http://load-test-service:8080"},
			map[string]string{"name": "CAPACITY_START_RPS", "value": "100"},
			map[string]string{"name": "CAPACITY_STEP_RPS", "value": "100"},
			map[string]string{"name": "CAPACITY_MAX_RPS", "value": "5000"},
			map[string]string{"name": "CAPACITY_CPU_LIMIT", "value": "90"},
		)
	}
	env = append(env, map[string]string{"name": "FAULT_TEST_TOKEN", "value": "ofm-test-fault-token"})
	container["env"] = env
	annotations := obj["metadata"].(map[string]any)["annotations"].(map[string]string)
	annotations["ofm.experiment.duration_seconds"] = strconv.Itoa(durationSeconds(in.Duration))
	annotations["ofm.experiment.load_mode"] = in.LoadMode
	if strings.EqualFold(in.LoadMode, "pc-capacity") {
		annotations["ofm.experiment.duration_seconds"] = "0"
	}
	annotations["ofm.experiment.workload"] = workload
	annotations["ofm.experiment.target_rps"] = strconv.Itoa(in.TargetRPS)
	var created jobObject
	if err := c.request(ctx, http.MethodPost, "/apis/batch/v1/namespaces/"+c.namespace+"/jobs", obj, &created); err != nil {
		if faultRestore != nil {
			faultRestore()
		}
		return application.Job{}, err
	}
	if faultRestore != nil {
		go func() {
			// A service-down experiment keeps the target unavailable while the
			// load is generated, then restores it during the final minute so
			// Kafka recovery and projections can converge before the run ends.
			d := 30 * time.Second
			if parsed, err := time.ParseDuration(in.FaultDuration); err == nil && parsed > 0 {
				d = parsed
			} else if strings.EqualFold(in.FaultProfile, "service-down") {
				d = time.Duration(durationSeconds(in.Duration)-60) * time.Second
				if d < 0 {
					d = 0
				}
			}
			c.log.Info("scheduled fault target restoration",
				zap.String("target", in.FaultTarget),
				zap.Duration("restore_after", d),
				zap.String("reason", "final-minute-recovery"),
			)
			timer := time.NewTimer(d)
			defer timer.Stop()
			<-timer.C
			faultRestore()
		}()
	}
	if faultAt > 0 {
		go func() {
			timer := time.NewTimer(faultAt)
			defer timer.Stop()
			<-timer.C
			restore, err := c.stopFaultTarget(context.Background(), in.FaultTarget)
			if err != nil {
				c.log.Error("delayed fault injection failed", zap.String("target", in.FaultTarget), zap.Error(err))
				return
			}
			d := 30 * time.Second
			if parsed, parseErr := time.ParseDuration(in.FaultDuration); parseErr == nil && parsed > 0 {
				d = parsed
			}
			restoreTimer := time.NewTimer(d)
			defer restoreTimer.Stop()
			<-restoreTimer.C
			restore()
		}()
	}
	return mapJob(created), nil
}

func (c *client) waitForKafkaReady(ctx context.Context) error {
	if strings.TrimSpace(c.kafkaBrokers) == "" {
		return errors.New("Kafka broker address is empty")
	}
	// The registration saga and CDC bridge can legitimately need several
	// minutes after a high-rate run.  The audit is the experiment's final
	// convergence gate, so do not snapshot the databases while outbox rows are
	// still pending.  Keep the bound finite so a broken consumer is reported.
	wait := 5 * time.Minute
	if seconds, err := strconv.Atoi(strings.TrimSpace(os.Getenv("OFM_PROJECTION_AUDIT_WAIT_SECONDS"))); err == nil && seconds > 0 {
		wait = time.Duration(seconds) * time.Second
	}
	deadline := time.Now().Add(wait)
	var lastErr error
	for time.Now().Before(deadline) {
		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err := kafka.DialContext(probeCtx, "tcp", c.kafkaBrokers)
		if err == nil {
			for _, topic := range []string{
				"saga.user.create",
				"saga.user.create.result",
				"saga.auth.create_pending_registration.result",
				"migration.registration.completed",
			} {
				if _, topicErr := conn.ReadPartitions(topic); topicErr != nil {
					err = fmt.Errorf("topic %s: %w", topic, topicErr)
					break
				}
			}
			_ = conn.Close()
		}
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
	if lastErr == nil {
		lastErr = errors.New("readiness deadline exceeded")
	}
	return fmt.Errorf("%s: %w", c.kafkaBrokers, lastErr)
}

func (c *client) stopFaultTarget(ctx context.Context, target string) (func(), error) {
	target = strings.TrimSpace(target)
	if strings.ContainsAny(target, "/") || target == "" {
		return nil, fmt.Errorf("invalid fault target %q", target)
	}
	path := "/apis/apps/v1/namespaces/" + c.namespace + "/deployments/" + target
	var deployment deploymentObject
	if err := c.request(ctx, http.MethodGet, path, nil, &deployment); err != nil {
		return nil, err
	}
	hpaPath := "/apis/autoscaling/v2/namespaces/" + c.namespace + "/horizontalpodautoscalers/" + target
	var hpa horizontalPodAutoscalerObject
	hpaEnabled := c.request(ctx, http.MethodGet, hpaPath, nil, &hpa) == nil
	hpaMinReplicas := 1
	if hpa.Spec.MinReplicas != nil {
		hpaMinReplicas = *hpa.Spec.MinReplicas
	}
	if hpaEnabled {
		if err := c.request(ctx, http.MethodPatch, hpaPath, map[string]any{"spec": map[string]int{"minReplicas": 0}}, nil); err != nil {
			return nil, fmt.Errorf("pause autoscaler for %q: %w", target, err)
		}
	}
	original := deployment.Spec.Replicas
	if original < 1 {
		original = 1
	}
	if err := c.request(ctx, http.MethodPatch, path, map[string]any{"spec": map[string]int{"replicas": 0}}, nil); err != nil {
		if hpaEnabled {
			_ = c.request(ctx, http.MethodPatch, hpaPath, map[string]any{"spec": map[string]int{"minReplicas": hpaMinReplicas}}, nil)
		}
		return nil, err
	}
	return func() {
		restoreCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := c.request(restoreCtx, http.MethodPatch, path, map[string]any{"spec": map[string]int{"replicas": original}}, nil); err != nil && c.log != nil {
			c.log.Error("restore mixed fault target failed", zap.String("deployment", target), zap.Error(err))
		}
		if hpaEnabled {
			if err := c.request(restoreCtx, http.MethodPatch, hpaPath, map[string]any{"spec": map[string]int{"minReplicas": hpaMinReplicas}}, nil); err != nil && c.log != nil {
				c.log.Error("restore autoscaler after fault failed", zap.String("deployment", target), zap.Error(err))
			}
		}
	}, nil
}
func valueOrEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func (c *client) List(ctx context.Context) ([]application.Job, error) {
	var result struct {
		Items []jobObject `json:"items"`
	}
	if err := c.request(ctx, http.MethodGet, "/apis/batch/v1/namespaces/"+c.namespace+"/jobs", nil, &result); err != nil {
		return nil, err
	}
	jobs := make([]application.Job, 0, len(result.Items))
	for _, j := range result.Items {
		if j.Metadata.Labels["ofm.test.run_id"] == "" {
			continue
		}
		job, err := c.Get(ctx, j.Metadata.Name)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}
func (c *client) Get(ctx context.Context, name string) (application.Job, error) {
	var j jobObject
	if err := c.request(ctx, http.MethodGet, "/apis/batch/v1/namespaces/"+c.namespace+"/jobs/"+name, nil, &j); err != nil {
		return application.Job{}, err
	}
	job := mapJob(j)
	if job.State == "FAILED" {
		if message := c.jobFailureMessage(ctx, name); message != "" {
			job.Message = message
		}
	}
	if message := j.Metadata.Annotations["ofm.results.message"]; message != "" {
		job.Message = message
	}
	if state := j.Metadata.Annotations["ofm.results.state"]; state == "PASS" || state == "FAILED" {
		job.State = state
	}
	terminal := j.Status.Succeeded > 0 || j.Status.Failed > 0
	if terminal && j.Metadata.Annotations["ofm.results.persisted"] != "true" {
		persistJob := job
		// mapJob intentionally exposes a succeeded job as RUNNING while the
		// asynchronous result persistence is in progress. That public state
		// must not be passed to persist, because persist uses the job state to
		// calculate the durable verdict in ClickHouse. Preserve the actual
		// Kubernetes terminal state for result persistence.
		if j.Status.Succeeded > 0 {
			persistJob.State = "COMPLETED"
		} else if j.Status.Failed > 0 {
			persistJob.State = "FAILED"
		}
		c.persistMu.Lock()
		_, alreadyPersisting := c.persisting[name]
		if !alreadyPersisting {
			c.persisting[name] = struct{}{}
		}
		c.persistMu.Unlock()
		if alreadyPersisting {
			job.State = "RUNNING"
			job.RemainingSeconds = 0
			job.Message = "Load finished; waiting for recovery evidence"
			return job, nil
		}
		// Kubernetes marks the k6 Job succeeded before asynchronous recovery
		// finishes. Keep the public experiment state non-terminal until the
		// result persistence path has evaluated Kafka completion/projection
		// evidence; otherwise the UI reports a false passing state.
		job.State = "RUNNING"
		job.RemainingSeconds = 0
		job.Message = "Load finished; waiting for recovery evidence"
		// Kafka evidence may arrive after the polling request. Persist in the
		// background so the status endpoint remains responsive during that wait.
		go func() {
			defer func() {
				c.persistMu.Lock()
				delete(c.persisting, name)
				c.persistMu.Unlock()
			}()
			persistCtx := context.Background()
			if err := c.persist(persistCtx, name, persistJob, j.Metadata.CreationTimestamp, j.Metadata.Annotations); err == nil {
				message := j.Metadata.Annotations["ofm.results.message"]
				if message == "" {
					message = resultMessageFromLog(context.Background(), c, name)
				}
				state := "PASS"
				if strings.HasPrefix(message, "FAILED:") {
					state = "FAILED"
				}
				annotations := map[string]string{"ofm.results.persisted": "true", "ofm.results.state": state}
				if message != "" {
					annotations["ofm.results.message"] = message
				}
				_ = c.request(context.Background(), http.MethodPatch, "/apis/batch/v1/namespaces/"+c.namespace+"/jobs/"+name, map[string]any{"metadata": map[string]any{"annotations": annotations}}, nil)
			} else if errors.Is(err, errRecoveryEvidencePending) {
				// Leave the annotation unset so a later status poll retries the
				// asynchronous recovery evidence scan.
				return
			} else if c.log != nil {
				c.log.Error("persisting load test result failed", zap.String("job", name), zap.Error(err))
			}
		}()
	}
	return job, nil
}
func (c *client) Stop(ctx context.Context, name string) error {
	return c.request(ctx, http.MethodDelete, "/apis/batch/v1/namespaces/"+c.namespace+"/jobs/"+name, map[string]string{"propagationPolicy": "Background"}, nil)
}
func mapJob(j jobObject) application.Job {
	state := "PENDING"
	msg := ""
	if j.Status.Active > 0 {
		state = "RUNNING"
	}
	if j.Status.Succeeded > 0 {
		state = "RUNNING"
	}
	if j.Status.Failed > 0 {
		state = "FAILED"
	}
	if len(j.Status.Conditions) > 0 {
		msg = j.Status.Conditions[len(j.Status.Conditions)-1].Message
	}
	duration := 0
	if j.Metadata.Annotations != nil {
		duration, _ = strconv.Atoi(j.Metadata.Annotations["ofm.experiment.duration_seconds"])
	}
	remaining := duration
	if started, err := time.Parse(time.RFC3339, j.Metadata.CreationTimestamp); err == nil && duration > 0 && state == "RUNNING" {
		remaining = duration - int(time.Since(started).Seconds())
		if remaining < 0 {
			remaining = 0
		}
	}
	return application.Job{Name: j.Metadata.Name, RunID: j.Metadata.Labels["ofm.test.run_id"], State: state, Message: msg, DurationSeconds: duration, RemainingSeconds: remaining}
}

func (c *client) jobFailureMessage(ctx context.Context, name string) string {
	if message := resultMessageFromLog(ctx, c, name); message != "" {
		return message
	}
	return "Experiment failed; see the k6 pod logs for the underlying error"
}

func resultMessageFromLog(ctx context.Context, c *client, name string) string {
	var pods struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	path := "/api/v1/namespaces/" + c.namespace + "/pods?labelSelector=job-name%3D" + url.QueryEscape(name)
	if err := c.request(ctx, http.MethodGet, path, nil, &pods); err != nil || len(pods.Items) == 0 {
		return ""
	}
	text, err := c.text(ctx, "/api/v1/namespaces/"+c.namespace+"/pods/"+pods.Items[0].Metadata.Name+"/log")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "{\"ofm_summary\":") {
			continue
		}
		var summary map[string]any
		if json.Unmarshal([]byte(line), &summary) == nil {
			if nested, ok := summary["ofm_summary"].(map[string]any); ok {
				summary = nested
			}
			return experimentResultMessage(summary)
		}
	}
	return ""
}

func experimentResultMessage(summary map[string]any) string {
	parts := make([]string, 0, 3)
	if failed := number(summary, "checks_failed"); failed > 0 {
		parts = append(parts, fmt.Sprintf("%d k6 checks failed", failed))
	}
	if passed, exists := summary["projection_audit_pass"]; exists && passed == false {
		parts = append(parts, "projection audit failed")
	}
	if _, exists := summary["full_system_completed_flows"]; exists && number(summary, "full_system_completed_flows") < 1 {
		parts = append(parts, "no full-system business flow completed")
	}
	if len(parts) == 0 {
		return "Experiment finished without reported k6 or projection failures"
	}
	return "FAILED: " + strings.Join(parts, "; ")
}

func durationSeconds(value string) int {
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return 0
	}
	return int(d.Seconds())
}
func newRunID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.New().String()
	}
	return id.String()
}

func (c *client) persist(ctx context.Context, name string, job application.Job, started string, annotations map[string]string) error {
	if c.clickhouse == "" {
		return nil
	}
	if started == "" {
		started = time.Now().UTC().Format(time.RFC3339Nano)
	}
	finished := time.Now().UTC().Format(time.RFC3339Nano)
	var pods struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := c.request(ctx, http.MethodGet, "/api/v1/namespaces/"+c.namespace+"/pods?labelSelector=job-name%3D"+url.QueryEscape(name), nil, &pods); err == nil && len(pods.Items) > 0 {
		if text, err := c.text(ctx, "/api/v1/namespaces/"+c.namespace+"/pods/"+pods.Items[0].Metadata.Name+"/log"); err == nil {
			for _, line := range strings.Split(text, "\n") {
				if strings.HasPrefix(line, "{\"ofm_summary\":") {
					var summary map[string]any
					if json.Unmarshal([]byte(line), &summary) == nil {
						if nested, ok := summary["ofm_summary"].(map[string]any); ok {
							summary = nested
						}
						return c.insertResult(ctx, started, finished, job, annotations, summary)
					}
				}
			}
		}
	}
	return c.insertResult(ctx, started, finished, job, annotations, map[string]any{})
}
func (c *client) insertResult(ctx context.Context, started, finished string, job application.Job, annotations map[string]string, summary map[string]any) error {
	if c.auditor != nil {
		var audit projectionaudit.Result
		var auditErr error
		if job.State == "FAILED" {
			// Failed k6 runs still need a factual PostgreSQL snapshot. Do one
			// read-only audit, but do not wait indefinitely for a projection set
			// that the failed workload may never have created.
			audit, auditErr = c.auditor.Audit(ctx, started, finished)
		} else {
			audit, auditErr = c.awaitProjectionAudit(started, finished)
		}
		if auditErr != nil {
			summary["projection_audit_pass"] = false
			summary["projection_audit_error"] = auditErr.Error()
			// Preserve the last factual sample even when convergence timed out.
			// An empty JSON document makes Grafana's JSONExtract functions render
			// every entity as zero and hides the actual CDC/projection gap.
			if encoded, marshalErr := json.Marshal(audit); marshalErr == nil {
				summary["projection_audit_json"] = string(encoded)
			}
		} else {
			summary["projection_audit_pass"] = audit.Pass
			encoded, _ := json.Marshal(audit)
			summary["projection_audit_json"] = string(encoded)
		}
	}
	if message := experimentResultMessage(summary); message != "" {
		annotations["ofm.results.message"] = message
	}
	if number(summary, "fallback_accepted") > 0 {
		// Only fallback requests can produce recovery.completed. Faults that
		// were injected but never reached the fallback path must not keep the
		// experiment waiting for impossible Kafka evidence.
		if evidence := c.awaitRecoveryEvidence(context.Background(), job.RunID, number(summary, "fallback_accepted")); evidence > 0 {
			summary["kafka_recovery_published"] = evidence
			summary["kafka_recovery_completed"] = evidence
			summary["projection_completed"] = evidence
		}
	}
	started = clickhouseTime(started)
	finished = clickhouseTime(finished)
	query := "INSERT INTO ofm_load_test_runs (started_at,finished_at,run_id,scenario_id,environment,profile,architecture,vus,rate,duration_seconds,status,checks_total,checks_failed,expected_failures,fault_injected,fallback_accepted,kafka_recovery_published,kafka_recovery_completed,projection_completed,load_shed,recovery_pass,verdict,http_requests,http_failed,p95_duration_ms,kafka_lag,projection_pending,projection_failures,dlq_count,recovery_time_ms,projection_audit_pass,projection_audit_json,result_json) FORMAT JSONEachRow"
	status := strings.ToLower(job.State)
	verdict := "FAIL"
	// Kubernetes completion is exposed as PASS after the result annotation is
	// persisted. Accept both representations so the ClickHouse verdict remains
	// consistent with the public API and Grafana dashboard.
	if (status == "completed" || status == "pass") && number(summary, "checks_failed") == 0 && recoveryEvidencePass(summary) {
		verdict = "PASS"
	}
	workload := annotations["ofm.experiment.workload"]
	if workload == "" {
		workload = "full-system"
	}
	projectionPass := summary["projection_audit_pass"] == true
	if !projectionPass {
		verdict = "FAIL"
	}
	if strings.EqualFold(workload, "full-system") && number(summary, "full_system_completed_flows") < 1 {
		verdict = "FAIL"
	}
	if verdict == "PASS" {
		status = "pass"
	} else {
		status = "failed"
	}
	row, _ := json.Marshal(map[string]any{"started_at": started, "finished_at": finished, "run_id": job.RunID, "scenario_id": workload + "-" + job.RunID, "environment": "k3d", "profile": annotations["ofm.experiment.profile"], "architecture": "microservice", "vus": annotationInt(annotations, "ofm.experiment.vus"), "rate": annotationInt(annotations, "ofm.experiment.target_rps"), "duration_seconds": number(summary, "test_run_duration_ms") / 1000, "status": status, "checks_total": number(summary, "checks_total"), "checks_failed": number(summary, "checks_failed"), "expected_failures": number(summary, "expected_failures"), "fault_injected": number(summary, "fault_injected"), "fallback_accepted": number(summary, "fallback_accepted"), "kafka_recovery_published": number(summary, "kafka_recovery_published"), "kafka_recovery_completed": number(summary, "kafka_recovery_completed"), "projection_completed": number(summary, "projection_completed"), "load_shed": number(summary, "load_shed"), "recovery_pass": boolToInt(recoveryEvidencePass(summary)), "verdict": verdict, "http_requests": number(summary, "http_requests"), "http_failed": number(summary, "http_failed"), "p95_duration_ms": summary["p95_duration_ms"], "kafka_lag": 0, "projection_pending": projectionPending(summary), "projection_failures": projectionFailures(summary), "dlq_count": 0, "recovery_time_ms": 0, "projection_audit_pass": boolToInt(projectionPass), "projection_audit_json": stringValue(summary, "projection_audit_json"), "result_json": stringValue(summary, "result_json")})
	// Result persistence must outlive the short HTTP polling request that
	// triggered Job completion; otherwise an async evidence scan cancels the
	// ClickHouse insert when the caller times out.
	persistCtx, persistCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer persistCancel()
	req, err := http.NewRequestWithContext(persistCtx, http.MethodPost, c.clickhouse+"/?query="+url.QueryEscape(query), bytes.NewReader(append(row, '\n')))
	if err != nil {
		return err
	}
	if c.clickhouseUser != "" {
		req.SetBasicAuth(c.clickhouseUser, c.clickhousePassword)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("clickhouse insert status %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	if c.log != nil {
		c.log.Info("load test result persisted", zap.String("run_id", job.RunID), zap.Int("http_status", resp.StatusCode))
	}
	// Fault injection alone does not imply that a fallback command exists:
	// requests may be rejected before the fallback boundary. Only keep the
	// control-plane job pending when k6 explicitly reports accepted fallback
	// work whose Kafka completion evidence can exist.
	// A Kubernetes-failed k6 job is already terminal. Missing recovery evidence
	// must not turn that failed run back into an indefinitely RUNNING control
	// plane job; the persisted audit and k6 summary are the failure evidence.
	if number(summary, "fallback_accepted") > 0 && !recoveryEvidencePass(summary) && c.log != nil {
		c.log.Warn("recovery evidence incomplete after bounded wait",
			zap.String("run_id", job.RunID),
			zap.Int("fallback_accepted", number(summary, "fallback_accepted")),
			zap.Int("recovery_completed", number(summary, "kafka_recovery_completed")))
	}
	return nil
}

// projectionPending exposes the aggregate CDC-to-monolith gap collected by the
// audit. It is intentionally derived from audit evidence because the service
// outbox has no delivery-status column.
func projectionPending(summary map[string]any) int64 {
	var total int64
	var audit projectionaudit.Result
	if json.Unmarshal([]byte(stringValue(summary, "projection_audit_json")), &audit) != nil {
		return 0
	}
	for _, entity := range audit.Entities {
		total += entity.OutboxPending
	}
	return total
}

// projectionFailures exposes entity parity mismatches as a bounded projection
// failure count until the projection journal is wired into the experiment
// service. Unlike the previous hard-coded zero, this preserves failed audit
// evidence in the run registry.
func projectionFailures(summary map[string]any) int64 {
	var total int64
	var audit projectionaudit.Result
	if json.Unmarshal([]byte(stringValue(summary, "projection_audit_json")), &audit) != nil {
		return 0
	}
	for _, entity := range audit.Entities {
		total += entity.Mismatches
	}
	return total
}

// awaitProjectionAudit gives CDC consumers a bounded drain window after k6
// stops. It repeats only read-only verification; it never repeats business
// requests or changes Kafka offsets.
func (c *client) awaitProjectionAudit(started, finished string) (projectionaudit.Result, error) {
	// A non-passing audit with pending rows or mismatches is not a failure yet:
	// it is the normal asynchronous CDC drain state. Keep sampling until the
	// audit converges. Infrastructure/query errors are returned immediately
	// below because they mean the projection cannot currently be verified.
	var last projectionaudit.Result
	var lastErr error
	wait := 5 * time.Minute
	if seconds, err := strconv.Atoi(strings.TrimSpace(os.Getenv("OFM_PROJECTION_AUDIT_WAIT_SECONDS"))); err == nil && seconds > 0 {
		wait = time.Duration(seconds) * time.Second
	}
	deadline := time.Now().Add(wait)
	for {
		if time.Now().After(deadline) {
			if lastErr != nil {
				return last, fmt.Errorf("projection audit timeout: %w", lastErr)
			}
			return last, fmt.Errorf("projection audit timeout: projection did not converge within %s", wait)
		}
		// PostgreSQL can take longer to establish/retry connections under the full
		// workload.  A short per-sample deadline cancels the audit itself and
		// leaves a misleading partial result instead of allowing convergence to
		// be observed.
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		resultCh := make(chan struct {
			result projectionaudit.Result
			err    error
		}, 1)
		go func() {
			result, err := c.auditor.Audit(ctx, started, finished)
			resultCh <- struct {
				result projectionaudit.Result
				err    error
			}{result: result, err: err}
		}()
		var result projectionaudit.Result
		var err error
		select {
		case outcome := <-resultCh:
			result, err = outcome.result, outcome.err
		case <-ctx.Done():
			cancel()
			lastErr = fmt.Errorf("projection audit sample timeout: %w", ctx.Err())
		}
		cancel()
		if err != nil {
			lastErr = err
		} else {
			last = result
			lastErr = nil
			if len(result.Errors) > 0 {
				return result, fmt.Errorf("projection audit failed: %s", strings.Join(result.Errors, "; "))
			}
			if result.Pass {
				return last, nil
			}
		}
		if lastErr != nil {
			return last, fmt.Errorf("projection audit failed: %w", lastErr)
		}
		timer := time.NewTimer(2 * time.Second)
		<-timer.C
	}
}

// awaitRecoveryEvidence waits for completed recovery events after k6 has
// stopped. Recovery is asynchronous, so HTTP response headers cannot prove
// Kafka delivery or projection completion for the original request.
func (c *client) awaitRecoveryEvidence(ctx context.Context, runID string, required int) int {
	if c.kafkaBrokers == "" || runID == "" || required <= 0 {
		return 0
	}
	// A fault can be observed by k6 without producing a fallback command (for
	// example, an invalid application request). In that case there is no
	// asynchronous recovery evidence to wait for; returning immediately keeps
	// the experiment result honest and prevents a ten-minute terminal hang.
	// Runs that actually accepted fallback still get the longer CDC drain
	// window below.
	// Recovery is intentionally asynchronous. Under load Debezium may drain an
	// existing outbox backlog before this run reaches the completed topic.
	// A missing completion event is a failed experiment signal, not a reason
	// to keep the control-plane job RUNNING for ten minutes.
	deadline, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	count := 0
	for {
		count = c.countCompletedEvents(deadline, runID)
		if count >= required || deadline.Err() != nil {
			return count
		}
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-deadline.Done():
			timer.Stop()
			return count
		case <-timer.C:
		}
	}
}

func (c *client) countCompletedEvents(ctx context.Context, runID string) int {
	conn, err := kafka.DialContext(ctx, "tcp", c.kafkaBrokers)
	if err != nil {
		if c.log != nil {
			c.log.Warn("kafka evidence bootstrap failed", zap.String("brokers", c.kafkaBrokers), zap.Error(err))
		}
		return 0
	}
	defer conn.Close()
	parts, err := conn.ReadPartitions("migration.recovery.completed")
	if err != nil {
		if c.log != nil {
			c.log.Warn("kafka evidence metadata failed", zap.Error(err))
		}
		return 0
	}
	identities := make(chan string, len(parts)*5000)
	var workers sync.WaitGroup
	for _, part := range parts {
		part := part
		workers.Add(1)
		go func() {
			defer workers.Done()
			partitionConn, dialErr := kafka.DialPartition(ctx, "tcp", c.kafkaBrokers, part)
			if dialErr != nil {
				if c.log != nil {
					c.log.Warn("kafka evidence partition dial failed", zap.Int("partition", part.ID), zap.Error(dialErr))
				}
				return
			}
			firstOffset, lastOffset, offsetErr := partitionConn.ReadOffsets()
			_ = partitionConn.Close()
			if offsetErr != nil || lastOffset <= firstOffset {
				return
			}
			startOffset := lastOffset - 5000
			if startOffset < firstOffset {
				startOffset = firstOffset
			}
			reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{c.kafkaBrokers}, Topic: "migration.recovery.completed", Partition: part.ID, StartOffset: startOffset, MinBytes: 1, MaxBytes: 4 << 20, MaxWait: 500 * time.Millisecond})
			readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
			defer readCancel()
			defer reader.Close()
			for {
				message, readErr := reader.ReadMessage(readCtx)
				if readErr != nil || message.Offset >= lastOffset {
					return
				}
				var event struct {
					CommandID string `json:"command_id"`
					TestRunID string `json:"test_run_id"`
				}
				if json.Unmarshal(message.Value, &event) != nil || event.TestRunID != runID {
					continue
				}
				identity := event.CommandID
				if identity == "" {
					identity = strconv.FormatInt(message.Offset, 10)
				}
				identities <- identity
			}
		}()
	}
	workers.Wait()
	close(identities)
	seen := make(map[string]struct{})
	for identity := range identities {
		seen[identity] = struct{}{}
	}
	count := len(seen)
	if c.log != nil {
		c.log.Info("kafka evidence count", zap.String("run_id", runID), zap.Int("count", count))
	}
	return count
}

func recoveryEvidencePass(summary map[string]any) bool {
	if number(summary, "fault_injected") == 0 {
		return true
	}
	return number(summary, "fallback_accepted") >= number(summary, "fault_injected") &&
		number(summary, "kafka_recovery_published") >= number(summary, "fault_injected") &&
		number(summary, "kafka_recovery_completed") >= number(summary, "fault_injected") &&
		number(summary, "projection_completed") >= number(summary, "fault_injected")
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
func annotationInt(annotations map[string]string, key string) int {
	v, _ := strconv.Atoi(annotations[key])
	return v
}
func clickhouseTime(value string) string {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return value
	}
	return parsed.UTC().Format("2006-01-02 15:04:05.000")
}
func number(m map[string]any, key string) int {
	switch v := m[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case json.Number:
		parsed, _ := v.Int64()
		return int(parsed)
	}
	return 0
}
func stringValue(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	b, _ := json.Marshal(m)
	return string(b)
}
func (c *client) text(ctx context.Context, path string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.api+path, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("kubernetes log status %s", resp.Status)
	}
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}
