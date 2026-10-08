# HFT And Market-Microstructure Fault Lab

The HFT lab tests resilience properties that generic latency proxies cannot understand. It is intended for simulated venues, captured fixtures, CI, interview projects, and pre-production trading infrastructure, never live capital without an independent safety review.

## Market Event Schema

One JSON object per line represents a venue and symbol stream. Supported event types are book updates, trades, order acknowledgements, cancel acknowledgements, and heartbeats. Prices use integer ticks and timestamps use integer nanoseconds to avoid floating-point ambiguity.

Seeded faults include deterministic and probabilistic drops, duplication, adjacent reordering, timestamp skew, acknowledgement delay, quantity corruption, and sequence-bounded venue partitions.

## Invariants

Replay reports:

- per-venue and per-symbol sequence gaps, duplicates, and out-of-order messages
- stale timestamps
- crossed order books
- rejected orders
- p50 and p99 acknowledgement latency in nanoseconds
- a deterministic SHA-256 digest of final book state

The baseline, faulted stream, and restored stream use the same input. A successful evidence run proves that the baseline is valid, the selected fault changed an observable result, and restoration exactly reproduces baseline state.

## FIX Capture Import

Import a text capture into the market-event schema, then replay it with the existing fault plans:

```bash
chaos hft import-fix tests/hft-evidence/executions.fix \
  --venue SIM --price-scale 2 --output market-events.jsonl
chaos hft replay market-events.jsonl \
  --fault-plan tests/hft-evidence/sequence-gap.yaml --output evidence.json
```

The importer accepts one pipe- or SOH-delimited FIX 4.4 execution report (`35=8`) per line. Blank lines are ignored; malformed or unsupported records fail the entire import with their original line number. Output is written only after every record has been converted.

| ExecType (150) | Replay event | Additional required fields |
| --- | --- | --- |
| `0` (New) | Accepted order acknowledgement | ClOrdID (11) |
| `8` (Rejected) | Rejected order acknowledgement | ClOrdID (11) |
| `4` (Canceled) | Cancel acknowledgement | ClOrdID (11) |
| `F` (Trade) | Trade | LastPx (31), LastQty (32) |

Every record requires MsgSeqNum (34), Symbol (55), and TransactTime (60). `--venue` identifies the captured session; no sorting, sequence renumbering, or timestamp adjustment occurs. Prices convert exactly to signed integer ticks: with `--price-scale 2`, `100.25` becomes `10025`. Precision that cannot be represented exactly is rejected. LastQty must be a positive integer because the replay schema does not support fractional quantities. UTC timestamps support whole seconds through nanoseconds and must fit the unsigned Unix-nanosecond schema.

Acknowledgement latency is set to zero as an **unmeasured placeholder**. A one-sided execution-report capture cannot establish round-trip latency; imported latency percentiles must not be treated as measured evidence. Use fixtures with measured acknowledgement latencies for latency-budget validation.

This is a supported-subset adapter, not a FIX session validator. It does not check BodyLength or CheckSum, reconstruct repeating groups, or support market-data snapshots, trade corrections, or mixed administrative messages. Use one session per capture. Replay sequence checks are per venue and symbol, while FIX MsgSeqNum is session-wide: filtering messages or interleaving symbols can therefore produce sequence gaps in replay. The checked-in fixture uses one symbol and contiguous session sequences.

The event mappings follow the [FIX order-state specification](https://www.fixtrading.org/online-specification/order-state-changes/); timestamp precision follows the [FIX time-precision addendum](https://www.fixtrading.org/packages/time-precision-technical-addendum/).

## FIX Session Faults

The FIX parser accepts SOH or pipe delimiters and produces a correct BodyLength and CheckSum when encoding. Fault plans can create sequence gaps, duplicate messages, set `PossDupFlag(43)`, corrupt checksums, or transform an order into an execution reject.

```bash
chaos hft fix tests/hft-evidence/orders.fix \
  --fault-plan tests/hft-evidence/fix-faults.yaml \
  --output faulted-orders.fix
```

## Research Direction

The next market-systems work is deliberately narrow and evidence-focused: PCAP/ITCH/OUCH import, deterministic exchange-clock models, cross-venue failover and cancel-on-disconnect assertions, strategy/risk invariant plugins, and latency attribution that separates network, queueing, matching, and application time.
