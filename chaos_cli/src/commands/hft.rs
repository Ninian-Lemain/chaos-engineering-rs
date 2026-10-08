use anyhow::{bail, Context, Result};
use chaos_hft::{
    evidence, import_fix_capture, FixFault, FixImportOptions, FixMessage, InvariantBudget,
    MarketEvent, MarketFaultPlan,
};
use clap::{Args, Subcommand};
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};

#[derive(Debug, Args)]
pub struct HftArgs {
    #[command(subcommand)]
    command: HftCommand,
}

#[derive(Debug, Subcommand)]
enum HftCommand {
    /// Convert supported FIX 4.4 execution reports to JSON Lines market events
    ImportFix {
        /// One pipe- or SOH-delimited FIX execution report per line
        input: PathBuf,
        /// Venue identifier for the captured session
        #[arg(long)]
        venue: String,
        /// Decimal places in integer price ticks (2 means one tick is 0.01)
        #[arg(long, default_value_t = 2)]
        price_scale: u32,
        /// Output JSON Lines fixture for hft replay
        #[arg(short, long)]
        output: PathBuf,
    },
    /// Replay market events through deterministic faults and prove restoration
    Replay {
        /// JSON Lines market-event fixture
        events: PathBuf,
        /// YAML or JSON market fault plan
        #[arg(long)]
        fault_plan: PathBuf,
        /// Optional YAML or JSON invariant and latency budget
        #[arg(long)]
        budget: Option<PathBuf>,
        /// Write the evidence report as JSON
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Require the faulted stream to remain within the supplied budget
        #[arg(long)]
        assert_chaos_budget: bool,
    },
    /// Apply session-aware sequence, duplicate, reject, and checksum faults to FIX messages
    Fix {
        /// Text file containing one pipe- or SOH-delimited FIX message per line
        input: PathBuf,
        /// YAML or JSON array of FIX faults
        #[arg(long)]
        fault_plan: PathBuf,
        /// Output transformed, pipe-delimited FIX messages
        #[arg(short, long)]
        output: PathBuf,
    },
}

pub async fn execute(args: HftArgs) -> Result<()> {
    match args.command {
        HftCommand::ImportFix {
            input,
            venue,
            price_scale,
            output,
        } => import_fix_events(&input, &output, venue, price_scale).await,
        HftCommand::Replay {
            events,
            fault_plan,
            budget,
            output,
            assert_chaos_budget,
        } => {
            replay_events(
                &events,
                &fault_plan,
                budget.as_deref(),
                output.as_deref(),
                assert_chaos_budget,
            )
            .await
        }
        HftCommand::Fix {
            input,
            fault_plan,
            output,
        } => apply_fix_faults(&input, &fault_plan, &output).await,
    }
}

async fn import_fix_events(
    input_path: &Path,
    output_path: &Path,
    venue: String,
    price_scale: u32,
) -> Result<()> {
    let contents = tokio::fs::read_to_string(input_path)
        .await
        .with_context(|| format!("read FIX capture from '{}'", input_path.display()))?;
    let events = import_fix_capture(&contents, &FixImportOptions { venue, price_scale })
        .with_context(|| format!("import FIX capture from '{}'", input_path.display()))?;
    let mut output = Vec::new();
    for event in &events {
        serde_json::to_writer(&mut output, event)?;
        output.push(b'\n');
    }
    write_file(output_path, &output).await?;
    println!(
        "Imported {} FIX execution reports into '{}'",
        events.len(),
        output_path.display()
    );
    Ok(())
}

async fn replay_events(
    events_path: &Path,
    fault_plan_path: &Path,
    budget_path: Option<&Path>,
    output_path: Option<&Path>,
    assert_chaos_budget: bool,
) -> Result<()> {
    let contents = tokio::fs::read_to_string(events_path)
        .await
        .with_context(|| format!("read market events from '{}'", events_path.display()))?;
    let events = contents
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            serde_json::from_str::<MarketEvent>(line).with_context(|| {
                format!(
                    "parse market event {} from '{}'",
                    index + 1,
                    events_path.display()
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if events.is_empty() {
        bail!("market event fixture is empty");
    }
    let plan: MarketFaultPlan = read_struct(fault_plan_path).await?;
    let budget: InvariantBudget = match budget_path {
        Some(path) => read_struct(path).await?,
        None => InvariantBudget::default(),
    };
    let report = evidence(&events, &plan, &budget)?;
    let json = serde_json::to_vec_pretty(&report)?;
    if let Some(path) = output_path {
        write_file(path, &json).await?;
    }
    println!("{}", String::from_utf8_lossy(&json));

    if !report.baseline.passed {
        bail!("baseline market stream violates its invariant budget");
    }
    if !report.disruption_observed {
        bail!("fault plan produced no measurable market-stream disruption");
    }
    if !report.restoration_verified || !report.restored.passed {
        bail!("market stream did not return to its exact baseline state");
    }
    if assert_chaos_budget && !report.chaos.passed {
        bail!(
            "faulted market stream exceeded its budget: {}",
            report.chaos.violations.join("; ")
        );
    }
    Ok(())
}

async fn apply_fix_faults(input_path: &Path, plan_path: &Path, output_path: &Path) -> Result<()> {
    let contents = tokio::fs::read_to_string(input_path)
        .await
        .with_context(|| format!("read FIX messages from '{}'", input_path.display()))?;
    let messages = contents
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            FixMessage::parse(line).with_context(|| {
                format!(
                    "parse FIX message {} from '{}'",
                    index + 1,
                    input_path.display()
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if messages.is_empty() {
        bail!("FIX message fixture is empty");
    }
    let faults: Vec<FixFault> = read_struct(plan_path).await?;
    let (output, impact) = FixFault::apply_all(&messages, &faults)?;
    if impact == Default::default() {
        bail!("FIX fault plan produced no protocol-visible effect");
    }
    let output = output
        .into_iter()
        .map(|message| message.replace('\u{1}', "|"))
        .collect::<Vec<_>>()
        .join("\n");
    write_file(output_path, output.as_bytes()).await?;
    println!("{}", serde_json::to_string_pretty(&impact)?);
    Ok(())
}

async fn read_struct<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let contents = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("read '{}'", path.display()))?;
    match path.extension().and_then(|value| value.to_str()) {
        Some("yaml" | "yml") => serde_yaml::from_str(&contents).map_err(Into::into),
        Some("json") => serde_json::from_str(&contents).map_err(Into::into),
        _ => bail!(
            "'{}' must use a .yaml, .yml, or .json extension",
            path.display()
        ),
    }
}

async fn write_file(path: &Path, contents: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, contents).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn imported_fix_fixture_proves_disruption_and_restoration() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let directory = std::env::temp_dir().join(format!(
            "chaos-fix-import-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let events = directory.join("market-events.jsonl");
        let output = directory.join("evidence.json");
        import_fix_events(
            &root.join("tests/hft-evidence/executions.fix"),
            &events,
            "SIM".into(),
            2,
        )
        .await
        .unwrap();
        replay_events(
            &events,
            &root.join("tests/hft-evidence/sequence-gap.yaml"),
            None,
            Some(&output),
            false,
        )
        .await
        .unwrap();
        let report: chaos_hft::ExperimentEvidence =
            serde_json::from_slice(&tokio::fs::read(&output).await.unwrap()).unwrap();
        assert_eq!(report.baseline.events, 4);
        assert!(report.baseline.passed);
        assert!(report.disruption_observed);
        assert!(report.restoration_verified);
        assert_eq!(report.chaos.sequence_gaps, 1);
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn invalid_fix_capture_preserves_existing_output() {
        let directory = std::env::temp_dir().join(format!(
            "chaos-fix-import-invalid-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let input = directory.join("capture.fix");
        let output = directory.join("events.jsonl");
        tokio::fs::write(
            &input,
            "8=FIX.4.4|35=8|34=1|55=ACME|11=order-1|150=0|60=20261008-09:00:00|\ninvalid\n",
        )
        .await
        .unwrap();
        tokio::fs::write(&output, "existing fixture").await.unwrap();
        let error = import_fix_events(&input, &output, "SIM".into(), 2)
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("line 2"));
        assert_eq!(
            tokio::fs::read_to_string(&output).await.unwrap(),
            "existing fixture"
        );
        tokio::fs::remove_dir_all(directory).await.unwrap();
    }

    #[tokio::test]
    async fn checked_in_hft_fixture_proves_disruption_and_restoration() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let output =
            std::env::temp_dir().join(format!("chaos-hft-evidence-{}.json", std::process::id()));
        replay_events(
            &root.join("tests/hft-evidence/market-events.jsonl"),
            &root.join("tests/hft-evidence/sequence-gap.yaml"),
            Some(&root.join("tests/hft-evidence/invariants.yaml")),
            Some(&output),
            false,
        )
        .await
        .unwrap();
        let report: chaos_hft::ExperimentEvidence =
            serde_json::from_slice(&tokio::fs::read(&output).await.unwrap()).unwrap();
        assert!(report.disruption_observed);
        assert!(report.restoration_verified);
        tokio::fs::remove_file(output).await.unwrap();
    }
}
