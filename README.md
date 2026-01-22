# SoionLab

[![CI](https://github.com/ZBaiY/SoionLab/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ZBaiY/SoionLab/actions/workflows/ci.yml?query=branch%3Amain)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)
![Platform](https://img.shields.io/badge/platform-Ubuntu%2022.04%20%7C%20macOS-9cf)

## What is SoionLab
SoionLab is a contract-driven research engine for **cross-domain market data** with heterogeneous timing and readiness semantics,with heterogeneous timing and readiness semantics, designed for execution-constrained, time-sensitive asynchronous systems. It keeps deterministic modeling boundaries by enforcing protocol interfaces and driver-owned time across backtest, mock, and realtime modes. The runtime treats time, lifecycle, and execution constraints as explicit research objects rather than implicit control flow.

Core research question: what is the robustness boundary under non-ideal data arrival (cross domain, ordering, frequency, completeness)?

## What is special: auditable execution risk
- Async hazard exposure (multi-source arrival mismatch) before a step is evaluated.
- Single time authority / driver-owned time to prevent lookahead-by-construction.
- Auditable failure surface: Hard Readiness vs Soft Degradation.

**Intended research domains**  
SoionLab explicitly targets challenging domains such as option chains and sentiment as *execution and timing stressors*, rather than as finished modeling features.  
These domains update asynchronously, lack closed-bar semantics, and may arrive incomplete. The current focus is on making their readiness, staleness, and absence explicit in the runtime, not on pricing or alpha extraction.


## 3-Min Quick Start
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt && pip install -e .
```

Run:
```bash
python apps/run_sample.py
```
Uses bundled data under `data/sample/` for demonstration only; intended to validate wiring + trace/log emission, not PnL. See [`docs/sample_data.md`](docs/sample_data.md) for scope and limitations.

### What you will see
- Console warnings such as `backtest.closed_bar.not_ready` or `soft_domain.not_ready` when readiness gates fail.
- Trace JSONL at `artifacts/runs/_current/logs/trace.jsonl` (or `artifacts/runs/<run_id>/logs/trace.jsonl`).
- Soft-readiness warnings appear as `soft_domain.not_ready` entries in `artifacts/runs/_current/logs/default.jsonl`; this run does not emit a full PnL report.

## System philosophy (brief)
SoionLab separates responsibilities so each layer can enforce its own invariants: strategies declare structure, data handlers guard snapshot legality, and the driver owns time. The engine composes components but does not infer timestamps or data provenance.

| Component | Responsibility | Owns time? | Invariant |
| --- | --- | --- | --- |
| Strategy | Declare structure, symbols, and wiring | No | No timestamps or I/O in the strategy definition. |
| DataHandler | Cache ticks and align snapshots | No | Snapshots only expose data with `data_ts <= step_ts`. |
| Driver | Advance time, ingest ticks, call `engine.step()` | Yes | Step timestamps are monotonic and driver-issued. |

## Deep dive docs
- [`docs/runtime_semantics.md`](docs/runtime_semantics.md): driver-owned time, lifecycle ordering, and runtime flow.
- [`docs/ingestion_boundary.md`](docs/ingestion_boundary.md): ingestion boundary, readiness contracts, and async replay.
- [`docs/contract_spec.md`](docs/contract_spec.md): protocol interfaces and cross-layer boundaries.

## Installation details
For conda/apt-get setup and fuller environment notes, see [`docs/INSTALL.md`](docs/INSTALL.md).
