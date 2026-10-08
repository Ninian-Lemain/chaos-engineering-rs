# Changelog

All notable changes are documented here. This project follows semantic versioning.

## Unreleased

### Added

- FIX 4.4 execution-report capture import into deterministic market-event JSONL, with exact decimal price scaling, UTC nanosecond timestamps, and replay disruption/restoration evidence.
- Capacity-zero pipeline replay that measures real producer backpressure without an intermediate queue.
- Generic JSONL integrity evidence for record loss, duplication, reordering, truncation, timestamp regression, field corruption, partition outage, sequence reset, cardinality explosion, and hot-key collapse.
- Searchable pipeline, CDC, WebSocket, gRPC, GraphQL, Redis/Valkey, OpenTelemetry, and crypto exchange scenario packs.
- Catalog pack kinds with production validation for both runnable scenarios and offline pipeline fault plans.

### Changed

- Project discovery now represents the complete API, infrastructure, distributed, streaming, observability, AI, and trading feature set rather than one specialty.

## 0.4.0 - 2026-08-13

### Added

- Mutual-TLS remote agents with authenticated prepare, execute, recover, ping, and emergency stop commands.
- Policy-gated distributed phases with deterministic per-target seeds and percentage/absolute blast-radius limits.
- Central SQLite experiment history with manifest and policy digests, SHA-256 artifacts, and age/count/byte retention.
- Reversible Kubernetes pod network isolation and Deployment/StatefulSet scale-to-zero faults with preflight RBAC checks.
- Deterministic HFT market-event replay with sequence, timestamp, crossed-book, rejection, and p50/p99 acknowledgement-latency invariants.
- FIX session parsing and seeded sequence-gap, duplication, PossDup, reject, and checksum faults.
- Stable evidence-backed trading scenario packs for market-data reordering, FIX corruption, and order-ack tail latency.
- `chaos agent`, `chaos distributed`, `chaos history`, and `chaos hft` command groups.

### Changed

- Project positioning now focuses on distributed and latency-critical systems, including quantitative trading infrastructure.
- Runtime capability metadata now includes the experimental `kubernetes_fault` injector.
- Distributed runs support cancellation-aware scenario cleanup and retain exact reproducibility metadata.

## 0.3.0 - 2026-08-13

### Added

- Opt-in CI suites that prove baseline health, disruption, and restoration against real PostgreSQL, MySQL, Kafka, RabbitMQ, NATS, MQTT, and S3-compatible services.
- Packet-level HLS and RTP fixtures for exact segment restoration and VP8/H.264 keyframe-loss assertions.
- OAuth/JWKS fixtures with real certificate rotation, overlap windows, and controllable token clocks.
- Versioned scenario-pack index metadata with CLI and scenario-schema compatibility constraints.
- Machine-readable `chaos list --json` and `chaos doctor --json` output.
- Deterministic `chaos list --json --platform <os>` views for release metadata generation.
- Runtime-generated README capability matrix with CI and release drift checks.

### Changed

- Stable scenario packs must map to CI evidence that asserts both disruption and restoration.
- Releases publish SHA-256 files and GitHub attestations for versioned pack archives and indexes.

## 0.2.1 - 2026-08-02

### Fixed

- Database I/O pressure now uses a bounded rolling write set and responds promptly to cancellation, preventing recovery from hanging behind long cycle delays or storage synchronization stalls.
- Homebrew tap CI now validates installation on both Apple Silicon and Intel runners through the same tap flow users run locally.

## 0.2.0 - 2026-08-02

### Added

- Runtime capability registry with stable, experimental, and planned injector states.
- Dependency and permission validation through `doctor` and `dry-run`.
- Recovery journal, interrupted-run recovery, and emergency `stop-all` cleanup.
- Rootless directional TCP proxy with latency, bandwidth, timeout, slow-close, data-limit, partition, corruption, duplication, reordering, and connection-limit faults.
- Rootless DNS and deterministic TLS fault endpoints.
- Provider-aware HTTP and AI proxy for OpenAI-compatible, Azure OpenAI, Anthropic, Gemini, OpenRouter, Ollama, Mistral, Groq, Cohere, Together, and vLLM traffic.
- Real Docker and Docker Compose pause, stop, kill, and restart targeting.
- Local DuckDB/SQLite file faults plus PostgreSQL/MySQL proxy scenarios.
- Windows service, file-lock, handle-pressure, and named-pipe faults.
- Prometheus endpoint, OTLP/HTTP export, SLO assertions, CI failure gates, and baseline-versus-chaos report comparison.
- Searchable downloadable scenario catalog covering AI, authentication, media, databases, queues, IoT, object storage, containers, network, and Windows.
- Reusable GitHub Action, multi-platform release binaries, GHCR image workflow, GitHub artifact attestations, and Homebrew/WinGet release templates.

### Changed

- Successful injection now requires a measurable real-world effect.
- Planned injectors fail closed instead of reporting simulated success.
- CPU, memory, disk, file-descriptor, process, and recovery behavior has stricter validation and integration coverage.
- Repository identity and links now consistently use `Ninian-Lemain`.

### Fixed

- Cross-platform filesystem arithmetic and Unix portability in CI.
- Web load-test rate and duration handling.
- Scenario parameter application and injection-failure reporting.
- Dashboard injector count and copyright year.

## 0.1.0

- Initial multi-crate chaos framework, CLI, scenarios, metrics, targets, and web dashboard.
