from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Mapping, Any, Optional
from quant_engine.data.derivatives.option_chain.snapshot import OptionChainSnapshot



def _atm_and_skew(
    *,
    spot: float | None,
    strike: pd.Series,
    iv: pd.Series,
    cp: pd.Series | None,
) -> tuple[float, float]:
    """Compute a simple ATM IV and a simple skew proxy.

    - ATM: strike closest to spot (or median strike if spot missing)
    - Skew: IV(0.9*spot put) - IV(1.1*spot call) when available, else local slope proxy
    """
    k = strike.astype("float64")
    v = iv.astype("float64")

    if spot is None or not np.isfinite(float(spot)) or float(spot) <= 0:
        # fallback: pick median strike as ATM (robust to pandas extension dtypes)
        k_np_all = k.to_numpy(dtype="float64", na_value=np.nan)
        k0 = float(np.nanmedian(k_np_all))
    else:
        k0 = float(spot)

    # ATM point
    idx_atm = (k - k0).abs().idxmin()
    atm_iv = float(v.loc[idx_atm])

    if spot is None or float(spot) <= 0 or cp is None:
        # fallback: slope of IV vs log-moneyness around ATM using nearest neighbors
        order = (k - k0).abs().sort_values().index
        sel = order[: min(7, len(order))]
        kk = k.loc[sel].to_numpy(dtype="float64", na_value=np.nan)
        vv = v.loc[sel].to_numpy(dtype="float64", na_value=np.nan)

        kk_pos = kk[np.isfinite(kk) & (kk > 0)]
        if len(kk_pos) >= 3:
            denom = float(k0)
            if (not np.isfinite(denom)) or denom <= 0:
                denom = float(np.nanmedian(kk_pos))

            x = np.log(kk_pos / denom)
            # align vv to the same positions as kk_pos
            vv_pos = vv[np.isfinite(kk) & (kk > 0)]
            m = np.isfinite(x) & np.isfinite(vv_pos)
            if int(m.sum()) >= 3:
                # linear fit: slope of IV vs log-moneyness around ATM
                slope, _intercept = np.polyfit(x[m], vv_pos[m], 1)
                return atm_iv, float(slope)
        return atm_iv, 0.0

    # target strikes
    k_put = 0.9 * float(spot)
    k_call = 1.1 * float(spot)

    cp_u = cp.astype("string")
    is_put = cp_u.str.upper().str.startswith("P")
    is_call = cp_u.str.upper().str.startswith("C")

    skew = 0.0
    try:
        if is_put.any() and is_call.any():
            kp = k[is_put]
            vp = v[is_put]
            kc = k[is_call]
            vc = v[is_call]
            ip = (kp - k_put).abs().idxmin()
            ic = (kc - k_call).abs().idxmin()
            skew = float(vp.loc[ip]) - float(vc.loc[ic])
    except Exception:
        skew = 0.0

    return atm_iv, float(skew)



def _smile_curve(*, spot: float | None, strike: pd.Series, iv: pd.Series) -> dict[str, float]:
    """Return a compact smile curve mapping moneyness (as str) -> iv.

    We store by log-moneyness rounded to 4dp to keep dict keys stable.
    """
    k = strike.astype("float64")
    v = iv.astype("float64")
    if spot is None or not np.isfinite(float(spot)) or float(spot) <= 0:
        # fallback: use strike itself as key
        out: dict[str, float] = {}
        for kk, vv in zip(k.values, v.values):
            if not np.isfinite(kk) or not np.isfinite(vv):
                continue
            out[f"K={kk:.2f}"] = float(vv)
        return out

    s0 = float(spot)
    out = {}
    for kk, vv in zip(k.values, v.values):
        if not np.isfinite(kk) or kk <= 0 or not np.isfinite(vv):
            continue
        m = float(np.log(kk / s0))
        out[f"m={m:.4f}"] = float(vv)
    return out


# =====================================================================================
# Reusable extraction for future IV calibration
# =====================================================================================

def extract_smile_inputs_from_chain_snapshot(
    chain_snap: OptionChainSnapshot,
    *,
    prefer_expiry: int | None = None,
) -> dict[str, Any] | None:
    """Extract a clean single-expiry smile slice for fitting/calibration.

    Returns None if required fields are missing.

    Output keys:
      - surface_ts: int
      - expiry_ts: int
      - spot: float | None
      - strike: np.ndarray[float]
      - iv: np.ndarray[float]
      - cp: np.ndarray[str] | None
    """
    surface_ts = int(getattr(chain_snap, "data_ts", getattr(chain_snap, "timestamp", 0)))
    frame = getattr(chain_snap, "frame", None)
    if frame is None or len(frame) == 0:
        return None

    strike = _get_num(frame, "strike")
    expiry_ts = _get_int(frame, "expiry_ts")
    cp = _get_str(frame, "cp")

    spot = _first_float(_get_num(frame, "price_index"))
    if spot is None:
        spot = _first_float(_get_num(frame, "index_price"))

    # IV preference order (all are expected to be stored as *_fetch keys)
    iv = _get_num(frame, "mark_iv_fetch")
    if iv is None:
        iv = _get_num(frame, "iv_fetch")
    if iv is None:
        bid_iv = _get_num(frame, "bid_iv_fetch")
        ask_iv = _get_num(frame, "ask_iv_fetch")
        if bid_iv is not None and ask_iv is not None:
            iv = (bid_iv + ask_iv) / 2.0

    if strike is None or expiry_ts is None or iv is None:
        return None

    exp_key = int(prefer_expiry) if prefer_expiry is not None else _pick_nearest_expiry(surface_ts, expiry_ts)
    if exp_key is None:
        return None

    m = (expiry_ts == int(exp_key))
    k = strike[m]
    v = iv[m]
    cpc = cp[m] if cp is not None else None

    ok = (k.notna()) & (v.notna()) & (k > 0) & (v > 0)
    if cpc is not None:
        ok = ok & cpc.notna()

    k = k[ok].astype("float64")
    v = v[ok].astype("float64")
    cpc_out = cpc[ok].astype("string") if cpc is not None else None

    if len(k) < 3:
        return None

    out: dict[str, Any] = {
        "surface_ts": surface_ts,
        "expiry_ts": int(exp_key),
        "spot": float(spot) if spot is not None else None,
        "strike": k.to_numpy(dtype="float64", na_value=np.nan),
        "iv": v.to_numpy(dtype="float64", na_value=np.nan),
        "cp": (cpc_out.to_numpy(dtype=object) if cpc_out is not None else None),
    }
    return out
def _coerce_ts(x: Any) -> int | None:
    if x is None:
        return None
    if isinstance(x, bool):  # bool is a subclass of int; exclude
        return None
    if isinstance(x, int):
        return int(x)
    if isinstance(x, float):
        # Allow float inputs but treat as ms epoch if already ms-like
        return int(x)
    if isinstance(x, dict):
        v = x.get("timestamp", x.get("ts"))
        if v is None:
            return None
        try:
            return int(v)
        except Exception:
            try:
                return int(float(v))
            except Exception:
                return None
    return None


# =====================================================================================
# frame helpers (OptionChainSnapshot.frame + aux dict)
# =====================================================================================

def _aux_series(frame: pd.DataFrame) -> pd.Series | None:
    try:
        return frame["aux"] if "aux" in frame.columns else None
    except Exception:
        return None


def _get_num(frame: pd.DataFrame, key: str) -> pd.Series | None:
    """Return float series for `key` from frame columns or aux dict."""
    if key in frame.columns:
        try:
            return pd.to_numeric(frame[key], errors="coerce")
        except Exception:
            return None

    aux = _aux_series(frame)
    if aux is None:
        return None
    try:
        s = aux.map(lambda d, k=key: d.get(k) if isinstance(d, dict) else None)
        return pd.to_numeric(s, errors="coerce")
    except Exception:
        return None


def _get_int(frame: pd.DataFrame, key: str) -> pd.Series | None:
    s = _get_num(frame, key)
    if s is None:
        return None
    try:
        return s.astype("Int64")
    except Exception:
        return None


def _get_str(frame: pd.DataFrame, key: str) -> pd.Series | None:
    if key in frame.columns:
        try:
            return frame[key].astype("string")
        except Exception:
            return None
    aux = _aux_series(frame)
    if aux is None:
        return None
    try:
        return aux.map(lambda d, k=key: d.get(k) if isinstance(d, dict) else None).astype("string")
    except Exception:
        return None


def _first_float(s: pd.Series | None) -> float | None:
    if s is None:
        return None
    try:
        x = s.dropna()
        if len(x) == 0:
            return None
        return float(x.iloc[0])
    except Exception:
        return None


def _pick_nearest_expiry(surface_ts: int, expiry_ts: pd.Series) -> int | None:
    """Pick the nearest expiry strictly after surface_ts if possible, else the nearest expiry.

    Avoid `np.unique(xs.values)` because pandas nullable integer dtypes may produce extension arrays.
    """
    try:
        xs = pd.to_numeric(expiry_ts, errors="coerce")
        xs = xs.dropna().astype("int64")
        if xs.empty:
            return None

        st = int(surface_ts)
        fut = xs[xs > st]
        if not fut.empty:
            return int(fut.min())
        return int(xs.min())
    except Exception:
        return None

