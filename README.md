# OFM Experiment Service

## Purpose

The Experiment Service provides the local experiment control plane. It starts and stops Kubernetes k6 Jobs, records run results in ClickHouse, and audits microservice-to-monolith projections. Status: active local tooling.

## Flow and boundaries

The HTTP UI/API accepts read/write load, duration, fault target, and fault profile. It creates a k6 Job, stops new requests when the configured load window ends, allows the projection grace period to drain, and calculates the final verdict from k6 checks and projection audit results.

The k6 workload is under k6/. It exercises the gateway and full business flow; it does not replace service-level tests. ClickHouse stores run history, Kubernetes stores active Jobs, and PostgreSQL/Kafka/Debezium data is audited by projection checks.

## Configuration

.env.example should explain Kubernetes namespace/client settings, ClickHouse URL and credentials, k6 image, runner limits, projection grace period, Kafka brokers, and audit database URLs. Image settings select the k6 and runner images; ClickHouse settings select result storage; grace-period values control draining after load stops. Secrets stay local.

## Local development

    go test ./...
    docker build -f Dockerfile .
    docker build -f k6/Dockerfile .

Run and deploy through the scripts and manifests in ofm-infra. The service is built as ofm/load-test-service:<tag> and the workload image as ofm/k6-full-system:<tag>.

## Observability and limitations

Use the experiment UI, ClickHouse run tables, Grafana dashboards, k6 output, OpenTelemetry traces, Kafka lag, and projection audit by entity. A successful k6 run does not alone prove projection correctness; the final verdict must include both checks and projection state.

