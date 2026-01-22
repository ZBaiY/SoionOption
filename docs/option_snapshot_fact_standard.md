# Option Snapshot Fact Standard

## Overview

This document defines the canonical schema for option chain snapshots in SoionOption.

## Status: DRAFT

This is a placeholder document. Full specification to be added.

## Core Schema (v2)

### OptionChainSnapshot

| Field | Type | Description |
|-------|------|-------------|
| data_ts | int | Event timestamp (epoch ms) |
| symbol | str | Base asset symbol (e.g., "BTC") |
| market | MarketSpec | Market/session metadata |
| domain | str | Always "option_chain" |
| schema_version | int | Schema version (currently 2) |
| frame | DataFrame | Normalized option chain table |

### Frame Columns (Core)

| Column | Type | Description |
|--------|------|-------------|
| instrument_name | str | Instrument identifier (e.g., "BTC-28JUN24-60000-C") |
| expiry_ts | int | Expiry timestamp (epoch ms) |
| strike | float | Strike price |
| cp | str | Call/Put indicator ("C" or "P") |
| aux | dict | Additional fields (IVs, greeks, etc.) |

### Aux Fields (Schema v2)

Fetched IV values are stored in `aux` with `*_fetch` suffix:

- `mark_iv_fetch`: Mark IV from exchange
- `bid_iv_fetch`: Bid IV from exchange
- `ask_iv_fetch`: Ask IV from exchange
- `iv_fetch`: Generic IV (fallback)

## IVSurfaceSnapshot

| Field | Type | Description |
|-------|------|-------------|
| data_ts | int | Data timestamp (epoch ms) |
| symbol | str | Base asset symbol |
| market | MarketSpec | Market metadata |
| atm_iv | float | ATM implied volatility |
| skew | float | IV skew metric |
| curve | dict | Moneyness -> IV mapping |
| surface | dict | Surface parameters |

## Quality Control Checks (TODO)

- [ ] Timestamp monotonicity
- [ ] Strike grid consistency
- [ ] Expiry alignment
- [ ] IV sanity bounds
- [ ] Arbitrage-free conditions

## References

- See `src/quant_engine/data/derivatives/option_chain/snapshot.py`
- See `src/quant_engine/data/derivatives/iv/snapshot.py`
