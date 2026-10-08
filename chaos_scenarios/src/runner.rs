use crate::{
    config::{InjectionConfig, Scenario, SloAssertionConfig},
    injector_factory::build_injector,
    scheduler::{Scheduler, SchedulingMode},
};
use chaos_core::{Executor, InjectionHandle, RecoveryJournal};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[derive(Default)]
pub struct RunTelemetry {
    active: AtomicU64,
    injections_attempted: AtomicU64,
    injections_succeeded: AtomicU64,
    cleanup_failures: AtomicU64,
    probes_total: AtomicU64,
    probes_failed: AtomicU64,
}

impl RunTelemetry {
    pub fn snapshot(&self) -> RunTelemetrySnapshot {
        RunTelemetrySnapshot {
            active: self.active.load(Ordering::Relaxed) != 0,
            injections_attempted: self.injections_attempted.load(Ordering::Relaxed),
            injections_succeeded: self.injections_succeeded.load(Ordering::Relaxed),
            cleanup_failures: self.cleanup_failures.load(Ordering::Relaxed),
            probes_total: self.probes_total.load(Ordering::Relaxed),
            probes_failed: self.probes_failed.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub struct RunTelemetrySnapshot {
    pub active: bool,
    pub injections_attempted: u64,
    pub injections_succeeded: u64,
    pub cleanup_failures: u64,
    pub probes_total: u64,
    pub probes_failed: u64,
}

struct ProbeSet {
    cancellation: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<SloResult>>,
}

impl ProbeSet {
    fn start(
        assertions: &[SloAssertionConfig],
        telemetry: Arc<RunTelemetry>,
    ) -> anyhow::Result<Self> {
        let cancellation = CancellationToken::new();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let tasks = assertions
            .iter()
            .map(|assertion| {
                let assertion = assertion.clone();
                let task_cancellation = cancellation.clone();
                let task_client = client.clone();
                let task_telemetry = telemetry.clone();
                tokio::spawn(async move {
                    run_probe(assertion, task_client, task_cancellation, task_telemetry).await
                })
            })
            .collect();
        Ok(Self {
            cancellation,
            tasks,
        })
    }

    async fn finish(self) -> Vec<SloResult> {
        self.cancellation.cancel();
        let mut results = Vec::with_capacity(self.tasks.len());
        for task in self.tasks {
            match task.await {
                Ok(result) => results.push(result),
                Err(error) => results.push(SloResult {
                    name: "probe-task".to_string(),
                    total_requests: 0,
                    failed_requests: 0,
                    error_rate: 1.0,
                    latency_p95: Duration::ZERO,
                    passed: false,
                    violations: vec![format!("Probe task failed: {}", error)],
                }),
            }
        }
        results
    }
}

async fn run_probe(
    assertion: SloAssertionConfig,
    client: reqwest::Client,
    cancellation: CancellationToken,
    telemetry: Arc<RunTelemetry>,
) -> SloResult {
    let samples = Arc::new(Mutex::new(ProbeSamples::default()));
    loop {
        let started = Instant::now();
        let response = tokio::time::timeout(assertion.timeout, client.get(&assertion.url).send());
        let outcome = tokio::select! {
            _ = cancellation.cancelled() => break,
            outcome = response => outcome,
        };
        let failed = !matches!(outcome, Ok(Ok(response)) if response.status().as_u16() == assertion.expected_status);
        telemetry.probes_total.fetch_add(1, Ordering::Relaxed);
        if failed {
            telemetry.probes_failed.fetch_add(1, Ordering::Relaxed);
        }
        {
            let mut samples = samples.lock().await;
            samples.latencies.push(started.elapsed());
            samples.failures += usize::from(failed);
        }
        tokio::select! {
            _ = cancellation.cancelled() => break,
            _ = tokio::time::sleep(assertion.interval) => {}
        }
    }
    let completed_samples = samples.lock().await.clone();
    evaluate_slo(&assertion, completed_samples)
}

#[derive(Clone, Default)]
struct ProbeSamples {
    latencies: Vec<Duration>,
    failures: usize,
}

fn evaluate_slo(assertion: &SloAssertionConfig, mut samples: ProbeSamples) -> SloResult {
    samples.latencies.sort();
    let total_requests = samples.latencies.len();
    let error_rate = if total_requests == 0 {
        1.0
    } else {
        samples.failures as f64 / total_requests as f64
    };
    let latency_p95 = percentile(&samples.latencies, 0.95);
    let mut violations = Vec::new();
    if total_requests < assertion.min_requests {
        violations.push(format!(
            "Only {} probe(s) completed; at least {} required",
            total_requests, assertion.min_requests
        ));
    }
    if error_rate > assertion.max_error_rate {
        violations.push(format!(
            "Error rate {:.4} exceeded {:.4}",
            error_rate, assertion.max_error_rate
        ));
    }
    if assertion
        .max_p95_latency
        .is_some_and(|threshold| latency_p95 > threshold)
    {
        violations.push(format!(
            "p95 latency {:?} exceeded {:?}",
            latency_p95,
            assertion.max_p95_latency.unwrap_or_default()
        ));
    }
    SloResult {
        name: assertion.name.clone(),
        total_requests,
        failed_requests: samples.failures,
        error_rate,
        latency_p95,
        passed: violations.is_empty(),
        violations,
    }
}

fn percentile(samples: &[Duration], quantile: f64) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    let index = ((samples.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(samples.len() - 1);
    samples[index]
}

pub struct ScenarioRunner {
    executor: Executor,
    telemetry: Arc<RunTelemetry>,
}

impl ScenarioRunner {
    pub fn new(executor: Executor) -> Self {
        Self {
            executor,
            telemetry: Arc::new(RunTelemetry::default()),
        }
    }

    pub fn with_telemetry(executor: Executor, telemetry: Arc<RunTelemetry>) -> Self {
        Self {
            executor,
            telemetry,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(Executor::with_defaults())
    }

    pub fn with_journal(journal: Arc<RecoveryJournal>) -> Self {
        Self::new(Executor::with_defaults_and_journal(journal))
    }

    pub fn with_journal_and_telemetry(
        journal: Arc<RecoveryJournal>,
        telemetry: Arc<RunTelemetry>,
    ) -> Self {
        Self::with_telemetry(Executor::with_defaults_and_journal(journal), telemetry)
    }

    pub fn telemetry(&self) -> Arc<RunTelemetry> {
        self.telemetry.clone()
    }

    pub async fn run(&self, scenario: &Scenario) -> anyhow::Result<ScenarioResult> {
        self.run_with_cancellation(scenario, CancellationToken::new())
            .await
    }

    pub async fn run_with_cancellation(
        &self,
        scenario: &Scenario,
        cancellation: CancellationToken,
    ) -> anyhow::Result<ScenarioResult> {
        info!("Starting scenario: {}", scenario.name);
        scenario.validate().map_err(|e| anyhow::anyhow!(e))?;

        self.telemetry.active.store(1, Ordering::Relaxed);
        let probes = ProbeSet::start(&scenario.assertions, self.telemetry.clone())?;

        let started_at = chrono::Utc::now();
        let start_time = Instant::now();

        // Phases are always sequential. A phase's `parallel` flag applies to the
        // injections inside that phase, which remain active together.
        let mut scheduler = Scheduler::new(SchedulingMode::Sequential, scenario.seed);

        let mut phases = scheduler.schedule_phases(scenario);

        if let Some(ramp_up) = scenario.ramp_up {
            scheduler.apply_ramp_up(&mut phases, ramp_up);
        }

        let mut phase_results = Vec::new();
        let mut total_attempted = 0;
        let mut total_succeeded = 0;
        let mut total_cleanup_failures = 0;
        let mut cancelled = false;

        // Execute phases
        for scheduled_phase in phases {
            if cancellation.is_cancelled() {
                cancelled = true;
                break;
            }
            // Wait until phase start time
            let elapsed = start_time.elapsed();
            if let Some(delay) = scheduled_phase.delay_until_start(elapsed) {
                info!(
                    "Waiting {:?} before starting phase '{}'",
                    delay,
                    scheduled_phase.name()
                );
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        cancelled = true;
                        break;
                    }
                    _ = tokio::time::sleep(delay) => {}
                }
            }

            info!(
                "Starting phase '{}' (duration: {:?})",
                scheduled_phase.name(),
                scheduled_phase.duration()
            );

            let phase_start = Instant::now();
            let mut handles = Vec::new();
            let mut injection_failures = Vec::new();
            let mut cleanup_failures = Vec::new();

            // Apply injections
            for injection in &scheduled_phase.phase.injections {
                total_attempted += 1;
                self.telemetry
                    .injections_attempted
                    .fetch_add(1, Ordering::Relaxed);
                match self.apply_injection(injection).await {
                    Ok(handle) => {
                        info!("Applied injection: {}", injection.r#type);
                        handles.push(handle);
                        total_succeeded += 1;
                        self.telemetry
                            .injections_succeeded
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        warn!("Failed to apply injection '{}': {}", injection.r#type, e);
                        injection_failures.push(InjectionFailure {
                            injection_type: injection.r#type.clone(),
                            error: e.to_string(),
                        });
                    }
                }
            }

            // Wait for phase duration
            let phase_elapsed = phase_start.elapsed();
            if phase_elapsed < scheduled_phase.duration() {
                let remaining = scheduled_phase.duration() - phase_elapsed;
                tokio::select! {
                    _ = cancellation.cancelled() => cancelled = true,
                    _ = tokio::time::sleep(remaining) => {}
                }
            }

            // Remove injections
            for handle in &handles {
                if let Err(e) = self.executor.remove(handle.clone()).await {
                    warn!("Failed to remove injection '{}': {}", handle.id, e);
                    total_cleanup_failures += 1;
                    self.telemetry
                        .cleanup_failures
                        .fetch_add(1, Ordering::Relaxed);
                    cleanup_failures.push(InjectionFailure {
                        injection_type: handle.injector_name.clone(),
                        error: e.to_string(),
                    });
                }
            }

            let phase_duration = phase_start.elapsed();
            info!(
                "Completed phase '{}' in {:?}",
                scheduled_phase.name(),
                phase_duration
            );

            phase_results.push(PhaseResult {
                name: scheduled_phase.name().to_string(),
                duration: phase_duration,
                injection_count: handles.len(),
                attempted_injections: scheduled_phase.phase.injections.len(),
                injection_failures,
                cleanup_failures,
            });

            if cancelled {
                break;
            }
        }

        let total_duration = start_time.elapsed();
        let slo_results = probes.finish().await;
        self.telemetry.active.store(0, Ordering::Relaxed);
        let telemetry = self.telemetry.snapshot();

        info!(
            "Scenario '{}' completed in {:?}",
            scenario.name, total_duration
        );

        Ok(ScenarioResult {
            scenario_name: scenario.name.clone(),
            started_at,
            total_duration,
            phase_results,
            total_injections: total_succeeded,
            attempted_injections: total_attempted,
            cleanup_failures: total_cleanup_failures,
            slo_results,
            telemetry,
            cancelled,
        })
    }

    async fn apply_injection(
        &self,
        injection: &InjectionConfig,
    ) -> anyhow::Result<InjectionHandle> {
        let target = injection
            .target
            .to_target()
            .map_err(|e| anyhow::anyhow!("Invalid target: {}", e))?;

        let handle = match build_injector(injection)? {
            Some(injector) => self.executor.inject_with(injector, &target).await,
            None => self.executor.inject(&injection.r#type, &target).await,
        }
        .map_err(|e| anyhow::anyhow!("Injection failed: {}", e))?;

        Ok(handle)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScenarioResult {
    pub scenario_name: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    #[serde(with = "humantime_serde")]
    pub total_duration: Duration,
    pub phase_results: Vec<PhaseResult>,
    pub total_injections: usize,
    #[serde(default)]
    pub attempted_injections: usize,
    #[serde(default)]
    pub cleanup_failures: usize,
    #[serde(default)]
    pub slo_results: Vec<SloResult>,
    #[serde(default)]
    pub telemetry: RunTelemetrySnapshot,
    #[serde(default)]
    pub cancelled: bool,
}

impl ScenarioResult {
    pub fn success_rate(&self) -> f64 {
        let attempted = if self.attempted_injections == 0 {
            self.total_injections
        } else {
            self.attempted_injections
        };

        if attempted == 0 {
            return 1.0;
        }

        self.total_injections as f64 / attempted as f64
    }

    pub fn average_phase_duration(&self) -> Duration {
        if self.phase_results.is_empty() {
            return Duration::ZERO;
        }

        let total: Duration = self.phase_results.iter().map(|p| p.duration).sum();
        total / self.phase_results.len() as u32
    }

    pub fn slos_passed(&self) -> bool {
        self.slo_results.iter().all(|result| result.passed)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SloResult {
    pub name: String,
    pub total_requests: usize,
    pub failed_requests: usize,
    pub error_rate: f64,
    #[serde(with = "humantime_serde")]
    pub latency_p95: Duration,
    pub passed: bool,
    #[serde(default)]
    pub violations: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PhaseResult {
    pub name: String,
    #[serde(with = "humantime_serde")]
    pub duration: Duration,
    pub injection_count: usize,
    #[serde(default)]
    pub attempted_injections: usize,
    #[serde(default)]
    pub injection_failures: Vec<InjectionFailure>,
    #[serde(default)]
    pub cleanup_failures: Vec<InjectionFailure>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct InjectionFailure {
    pub injection_type: String,
    pub error: String,
}

pub async fn run_scenario(scenario: &Scenario) -> anyhow::Result<ScenarioResult> {
    let runner = ScenarioRunner::with_defaults();
    runner.run(scenario).await
}

mod humantime_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&humantime::format_duration(*duration).to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        humantime::parse_duration(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    async fn status_server(status: u16) -> (String, CancellationToken) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    accepted = listener.accept() => accepted,
                };
                let Ok((mut stream, _)) = accepted else {
                    continue;
                };
                tokio::spawn(async move {
                    let response = format!(
                        "HTTP/1.1 {} Test\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                        status
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        (format!("http://{}", address), cancellation)
    }

    #[test]
    fn test_scenario_runner_creation() {
        let _runner = ScenarioRunner::with_defaults();
    }

    #[test]
    fn test_scenario_result() {
        let result = ScenarioResult {
            scenario_name: "test".to_string(),
            started_at: chrono::Utc::now(),
            total_duration: Duration::from_secs(100),
            phase_results: vec![
                PhaseResult {
                    name: "phase1".to_string(),
                    duration: Duration::from_secs(50),
                    injection_count: 2,
                    attempted_injections: 2,
                    injection_failures: vec![],
                    cleanup_failures: vec![],
                },
                PhaseResult {
                    name: "phase2".to_string(),
                    duration: Duration::from_secs(50),
                    injection_count: 1,
                    attempted_injections: 2,
                    injection_failures: vec![InjectionFailure {
                        injection_type: "unknown".to_string(),
                        error: "not registered".to_string(),
                    }],
                    cleanup_failures: vec![],
                },
            ],
            total_injections: 3,
            attempted_injections: 4,
            cleanup_failures: 0,
            slo_results: vec![],
            telemetry: RunTelemetrySnapshot::default(),
            cancelled: false,
        };

        assert_eq!(result.success_rate(), 0.75);
        assert_eq!(result.average_phase_duration(), Duration::from_secs(50));
    }

    #[tokio::test]
    async fn failing_http_slo_is_recorded_in_result() {
        let (url, stop_server) = status_server(503).await;
        let scenario = Scenario::builder()
            .name("slo failure")
            .add_phase(
                crate::config::Phase::builder()
                    .name("observe")
                    .duration(Duration::from_millis(150))
                    .build(),
            )
            .add_assertion(SloAssertionConfig {
                name: "availability".to_string(),
                url,
                expected_status: 200,
                interval: Duration::from_millis(20),
                timeout: Duration::from_millis(50),
                max_error_rate: 0.0,
                max_p95_latency: Some(Duration::from_millis(100)),
                min_requests: 3,
            })
            .build();

        let result = ScenarioRunner::with_defaults()
            .run(&scenario)
            .await
            .unwrap();
        assert!(!result.slos_passed());
        assert!(result.slo_results[0].total_requests >= 3);
        assert_eq!(result.slo_results[0].error_rate, 1.0);
        assert!(!result.slo_results[0].violations.is_empty());
        stop_server.cancel();
    }
}
