//! Deterministic fault replay and evidence for latency-critical trading systems.

mod fault;
mod fix;
mod import;
mod replay;

pub use fault::{FaultImpact, MarketFault, MarketFaultPlan};
pub use fix::{FixFault, FixFaultImpact, FixMessage};
pub use import::{import_fix_capture, FixImportOptions};
pub use replay::{
    evidence, replay, BookAction, EventKind, ExperimentEvidence, InvariantBudget, MarketEvent,
    ReplayReport, Side,
};
