use crate::{EventKind, FixMessage, MarketEvent};
use anyhow::{anyhow, ensure, Context};
use chrono::NaiveDateTime;

/// Configuration for importing the supported FIX execution-report subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixImportOptions {
    /// Venue assigned to every imported event; must contain non-whitespace text.
    pub venue: String,
    /// Number of decimal places in a price tick representation, from 0 through 9.
    pub price_scale: u32,
}

/// Import one pipe- or SOH-delimited FIX.4.4 execution report per nonblank line.
///
/// Supports ExecType (150) `0` (new), `8` (rejected), `4` (canceled), and `F`
/// (trade). All reports require Symbol (55), a positive MsgSeqNum (34), and
/// TransactTime (60) in UTC `YYYYMMDD-HH:MM:SS[.fffffffff]` format. Acknowledgements
/// require ClOrdID (11); trades require LastPx (31) and positive integer LastQty
/// (32). Prices are converted exactly into signed ticks using `price_scale`.
///
/// Acknowledgement latency is set to zero as an **unmeasured placeholder**:
/// execution reports alone cannot establish request-to-acknowledgement latency.
/// Imported baseline latency statistics therefore do not measure venue latency.
/// This is a supported-subset importer, not a full FIX validator: it does not
/// validate BodyLength (9), CheckSum (10), or other full-protocol requirements.
/// Required singleton tags must occur exactly once. Errors identify the original
/// line number, including any skipped blank lines.
pub fn import_fix_capture(
    contents: &str,
    options: &FixImportOptions,
) -> anyhow::Result<Vec<MarketEvent>> {
    ensure!(!options.venue.trim().is_empty(), "venue cannot be empty");
    ensure!(
        options.price_scale <= 9,
        "price_scale must be between 0 and 9"
    );
    let mut events = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        events.push(
            import_line(line, options)
                .with_context(|| format!("FIX capture line {}", index + 1))?,
        );
    }
    ensure!(!events.is_empty(), "FIX capture contains no messages");
    Ok(events)
}

fn import_line(line: &str, options: &FixImportOptions) -> anyhow::Result<MarketEvent> {
    ensure!(
        !(line.contains('|') && line.contains('\u{1}')),
        "mixed FIX delimiters are unsupported"
    );
    let message = FixMessage::parse(line)?;
    ensure!(
        required(&message, 8)? == "FIX.4.4",
        "only FIX.4.4 is supported"
    );
    ensure!(
        required(&message, 35)? == "8",
        "only ExecutionReport MsgType (35) 8 is supported"
    );
    let sequence = positive_integer(required(&message, 34)?, "MsgSeqNum (34)")?;
    let symbol = required(&message, 55)?.to_owned();
    let timestamp_ns = timestamp(required(&message, 60)?)?;
    let kind = match required(&message, 150)? {
        exec_type @ ("0" | "8") => EventKind::OrderAck {
            client_order_id: required(&message, 11)?.to_owned(),
            accepted: exec_type == "0",
            latency_ns: 0,
        },
        "4" => EventKind::CancelAck {
            client_order_id: required(&message, 11)?.to_owned(),
            latency_ns: 0,
        },
        "F" => EventKind::Trade {
            price_ticks: price(required(&message, 31)?, options.price_scale)?,
            quantity: positive_integer(required(&message, 32)?, "LastQty (32)")?,
        },
        other => return Err(anyhow!("unsupported ExecType (150) '{other}'")),
    };
    Ok(MarketEvent {
        sequence,
        timestamp_ns,
        venue: options.venue.clone(),
        symbol,
        kind,
    })
}

fn required(message: &FixMessage, tag: u32) -> anyhow::Result<&str> {
    let mut values = message
        .fields
        .iter()
        .filter(|(candidate, _)| *candidate == tag);
    let (_, value) = values
        .next()
        .ok_or_else(|| anyhow!("missing required FIX tag {tag}"))?;
    ensure!(values.next().is_none(), "duplicate required FIX tag {tag}");
    ensure!(!value.trim().is_empty(), "empty required FIX tag {tag}");
    Ok(value)
}

fn positive_integer(value: &str, name: &str) -> anyhow::Result<u64> {
    ensure!(
        value.bytes().all(|byte| byte.is_ascii_digit()),
        "{name} must be a positive integer"
    );
    let parsed: u64 = value.parse().with_context(|| format!("invalid {name}"))?;
    ensure!(parsed > 0, "{name} must be a positive integer");
    Ok(parsed)
}

fn timestamp(value: &str) -> anyhow::Result<u64> {
    let bytes = value.as_bytes();
    ensure!(
        bytes.len() == 17 || (19..=27).contains(&bytes.len()),
        "TransactTime (60) must be YYYYMMDD-HH:MM:SS with optional 1..9 fractional digits"
    );
    ensure!(
        bytes.iter().enumerate().all(|(index, byte)| match index {
            8 => *byte == b'-',
            11 | 14 => *byte == b':',
            17 => *byte == b'.',
            _ => byte.is_ascii_digit(),
        }),
        "invalid TransactTime (60) format"
    );
    let format = if bytes.len() == 17 {
        "%Y%m%d-%H:%M:%S"
    } else {
        "%Y%m%d-%H:%M:%S%.f"
    };
    let date = NaiveDateTime::parse_from_str(value, format)
        .context("invalid TransactTime (60) date or time")?
        .and_utc();
    ensure!(
        date.timestamp_subsec_nanos() < 1_000_000_000,
        "TransactTime (60) leap seconds are unsupported"
    );
    let seconds =
        u64::try_from(date.timestamp()).context("TransactTime (60) precedes Unix epoch")?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|ns| ns.checked_add(u64::from(date.timestamp_subsec_nanos())))
        .ok_or_else(|| anyhow!("TransactTime (60) exceeds Unix nanosecond range"))
}

fn price(value: &str, scale: u32) -> anyhow::Result<i64> {
    let (negative, unsigned) = match value.strip_prefix('-') {
        Some(unsigned) => (true, unsigned),
        None => (false, value),
    };
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    ensure!(
        !whole.is_empty() && whole.bytes().all(|byte| byte.is_ascii_digit()),
        "invalid LastPx (31)"
    );
    ensure!(
        fraction.bytes().all(|byte| byte.is_ascii_digit())
            && (!unsigned.contains('.') || !fraction.is_empty()),
        "invalid LastPx (31)"
    );
    let scale = scale as usize;
    ensure!(
        fraction.bytes().skip(scale).all(|byte| byte == b'0'),
        "LastPx (31) exceeds configured price precision"
    );
    let limit = if negative {
        i64::MAX as u64 + 1
    } else {
        i64::MAX as u64
    };
    let mut magnitude = 0u64;
    for digit in whole
        .bytes()
        .chain(fraction.bytes().take(scale))
        .chain(std::iter::repeat_n(
            b'0',
            scale.saturating_sub(fraction.len()),
        ))
    {
        magnitude = magnitude
            .checked_mul(10)
            .and_then(|current| current.checked_add(u64::from(digit - b'0')))
            .filter(|current| *current <= limit)
            .ok_or_else(|| anyhow!("LastPx (31) exceeds signed tick range"))?;
    }
    if negative && magnitude == i64::MAX as u64 + 1 {
        Ok(i64::MIN)
    } else {
        let ticks = i64::try_from(magnitude).context("LastPx (31) exceeds signed tick range")?;
        Ok(if negative { -ticks } else { ticks })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{evidence, InvariantBudget, MarketFault, MarketFaultPlan};

    fn options() -> FixImportOptions {
        FixImportOptions {
            venue: "XNAS".into(),
            price_scale: 2,
        }
    }

    fn report(sequence: u64, execution: &str, fields: &str) -> String {
        format!("8=FIX.4.4|35=8|34={sequence}|55=ACME|60=19700101-00:00:01.123456789|150={execution}|{fields}|")
    }

    #[test]
    fn imports_supported_reports_and_soh() {
        let capture = [
            report(1, "0", "11=new"),
            report(2, "8", "11=reject"),
            report(3, "4", "11=cancel"),
            report(4, "F", "31=12.34000|32=7").replace('|', "\u{1}"),
        ]
        .join("\n");
        let events = import_fix_capture(&capture, &options()).unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].timestamp_ns, 1_123_456_789);
        assert_eq!(events[0].venue, "XNAS");
        assert_eq!(events[0].symbol, "ACME");
        assert_eq!(
            events[0].kind,
            EventKind::OrderAck {
                client_order_id: "new".into(),
                accepted: true,
                latency_ns: 0
            }
        );
        assert_eq!(
            events[1].kind,
            EventKind::OrderAck {
                client_order_id: "reject".into(),
                accepted: false,
                latency_ns: 0
            }
        );
        assert_eq!(
            events[2].kind,
            EventKind::CancelAck {
                client_order_id: "cancel".into(),
                latency_ns: 0
            }
        );
        assert_eq!(
            events[3].kind,
            EventKind::Trade {
                price_ticks: 1234,
                quantity: 7
            }
        );
    }

    #[test]
    fn converts_price_exactly_at_signed_boundaries() {
        for (value, scale, expected) in [
            ("92233720368547758.07", 2, i64::MAX),
            ("-92233720368547758.08", 2, i64::MIN),
            ("1.000000001", 9, 1_000_000_001),
            ("-0.01", 2, -1),
            ("10.000", 0, 10),
        ] {
            assert_eq!(price(value, scale).unwrap(), expected, "{value}");
        }
        for value in [
            "92233720368547758.08",
            "-92233720368547758.09",
            "1.001",
            "NaN",
            "1e2",
            "1.",
            ".1",
            "+1",
            "--1",
            "1.2.3",
        ] {
            assert!(price(value, 2).is_err(), "{value}");
        }
    }

    #[test]
    fn accepts_exact_utc_timestamps_and_checks_range() {
        assert_eq!(timestamp("19700101-00:00:00").unwrap(), 0);
        assert_eq!(timestamp("19700101-00:00:00.1").unwrap(), 100_000_000);
        assert_eq!(timestamp("25540721-23:34:33.709551615").unwrap(), u64::MAX);
        for value in [
            "25540721-23:34:33.709551616",
            "19691231-23:59:59",
            "19700101-00:00:00.",
            "19700101-00:00:00.1234567890",
            "19700101-00:00:00Z",
            "1970-01-01T00:00:00",
            "20230229-00:00:00",
            "20161231-23:59:60",
            "19700101-24:00:00",
        ] {
            assert!(timestamp(value).is_err(), "{value}");
        }
    }

    #[test]
    fn rejects_ambiguous_missing_malformed_and_unsupported_fields() {
        let valid = report(1, "F", "31=12.34|32=7");
        let mut invalid = vec![
            report(0, "0", "11=id"),
            report(1, "0", "58=no-id"),
            report(1, "F", "31=1|32=0"),
            report(1, "F", "31=1|32=1.5"),
            report(1, "F", "31=1|32=+1"),
            report(1, "F", "31=1|32=18446744073709551616"),
            report(1, "G", "11=id"),
            valid.replace("35=8", "35=D"),
            valid.replace("FIX.4.4", "FIX.4.2"),
            valid.replace("55=ACME|", ""),
            valid.replace("55=ACME", "55=  "),
            valid.replace("34=1", "34=-1"),
            valid.replace("31=12.34", "31"),
            valid.replace("|35=8|", "\u{1}35=8|"),
        ];
        for tag in [8, 35, 34, 55, 60, 150, 31, 32] {
            invalid.push(format!("{valid}{tag}=duplicate|"));
        }
        invalid.push(report(1, "0", "11=id|11=duplicate"));
        for capture in invalid {
            let error = import_fix_capture(&format!("\n\n{capture}"), &options()).unwrap_err();
            assert!(
                format!("{error:#}").contains("line 3"),
                "{capture}: {error:#}"
            );
        }
    }

    #[test]
    fn rejects_empty_capture_and_invalid_options() {
        assert!(import_fix_capture("\n \r\n", &options()).is_err());
        let capture = report(1, "0", "11=id");
        assert!(import_fix_capture(
            &capture,
            &FixImportOptions {
                venue: " ".into(),
                price_scale: 2
            }
        )
        .is_err());
        assert!(import_fix_capture(
            &capture,
            &FixImportOptions {
                venue: "XNAS".into(),
                price_scale: 10
            }
        )
        .is_err());
    }

    #[test]
    fn imported_capture_proves_disruption_and_restoration() {
        let capture = (1..=3)
            .map(|sequence| report(sequence, "0", "11=id"))
            .collect::<Vec<_>>()
            .join("\n");
        let events = import_fix_capture(&capture, &options()).unwrap();
        let plan = MarketFaultPlan {
            seed: 42,
            faults: vec![MarketFault::DropEvery {
                every: 3,
                offset: 1,
            }],
        };
        let result = evidence(&events, &plan, &InvariantBudget::default()).unwrap();
        assert!(result.baseline.passed);
        assert_eq!(result.chaos.sequence_gaps, 1);
        assert!(!result.chaos.passed);
        assert!(result.disruption_observed);
        assert!(result.restoration_verified);
    }
}
