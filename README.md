# SoionOption (Research)

![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)
![Platform](https://img.shields.io/badge/platform-Ubuntu%2022.04%20%7C%20macOS-9cf)

## What is SoionOption

SoionOption is a **focused research repository** extracted from SoionLab, dedicated to experimenting with the **option chain and IV data layer only**.

This repo is **not** a full trading engine, runtime, or strategy framework.
It exists to:
- validate option-chain snapshot semantics,
- stress-test data quality control (QC) logic,
- study uncertainty propagation and regime stability,
in isolation from execution, async ingestion, or strategy logic.

The code here is intentionally minimal and deterministic.

## Scope (What this repo does)

- Batch-style, deterministic experiments on **option chain snapshots**
- Explicit handling of:
  - `step_ts` (evaluation anchor)
  - `market_ts`
  - `arrival_ts`
- QC gating that may explicitly **reject** data (`HARD_FAIL`) instead of producing signals
- Monte Carlo used only as **observation uncertainty propagation** (not pricing)
- Regime detection as a **state object**, not a label

## Non-scope (What this repo does NOT do)

- No async ingestion
- No runtime / driver / engine
- No strategy execution
- No portfolio, risk, or order routing
- No alpha or PnL optimization

Any such code that existed in SoionLab is either removed or kept only as non-importable reference.

## Relationship to SoionLab

This repository is a **sandbox**.

Many research modules include explicit comments like:

```
# RESEARCH-ONLY MODULE
# Intended final location: src/quant_engine/data/derivatives/option_chain/...
```

indicating where the code is expected to be migrated once stabilized.

Frozen contracts (ticks, caches, snapshots, option_chain and IV handlers) are imported directly from the original SoionLab layout and are **not modified** here.

## Quick start (research module only)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

## Design principle

> If the system is not confident, it must stop.

Rejection, degradation, and explicit `Unknown / NaN` outputs are first-class outcomes, not errors.

## Status

- [x] Repo scaffolding
- [x] Frozen data contracts imported
- [ ] Route A: QC Gate (in progress)
- [ ] MC uncertainty propagation
- [ ] Regime detector
- [ ] Continuity / arbitrage constraints
- [ ] Optional surface consistency checks
