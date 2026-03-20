"""
Ψ-jam Analysis Module
Physics-based market analysis: Kramers-Moyal extraction, Langevin diagnostics,
Hurst/Lyapunov exponents, RQA, VPIN, and regime detection.
"""

import numpy as np
from scipy import stats
from scipy.spatial.distance import pdist, squareform
from typing import Optional


# ═══════════════════════════════════════════════════
# KRAMERS-MOYAL COEFFICIENT EXTRACTION
# ═══════════════════════════════════════════════════

def kramers_moyal_coefficients(
    x: np.ndarray,
    dt: float = 1.0,
    bins: int = 50,
    max_order: int = 4,
) -> dict:
    """
    Extract Kramers-Moyal coefficients D1 (drift), D2 (diffusion), D4
    from a time series using conditional moments.

    D^(n)(x) = (1/n!) * lim_{τ→0} (1/τ) * <(x(t+τ) - x(t))^n | x(t) = x>

    Returns dict with bin_centers, D1, D2, D4, and diagnostics.
    """
    x = np.asarray(x, dtype=float)
    dx = np.diff(x)

    # Bin the state space
    x_bins = np.linspace(np.percentile(x[:-1], 2), np.percentile(x[:-1], 98), bins)
    bin_width = x_bins[1] - x_bins[0]
    bin_centers = []
    D1, D2, D4 = [], [], []
    counts = []

    for i in range(len(x_bins) - 1):
        mask = (x[:-1] >= x_bins[i]) & (x[:-1] < x_bins[i + 1])
        n = mask.sum()
        if n < 10:
            continue

        center = (x_bins[i] + x_bins[i + 1]) / 2
        increments = dx[mask]

        m1 = np.mean(increments) / dt
        m2 = np.mean(increments**2) / dt
        m4 = np.mean(increments**4) / dt

        bin_centers.append(center)
        D1.append(m1)
        D2.append(m2 / 2)  # D2 = M2/2
        D4.append(m4 / 24)  # D4 = M4/24
        counts.append(n)

    bin_centers = np.array(bin_centers)
    D1 = np.array(D1)
    D2 = np.array(D2)
    D4 = np.array(D4)

    # Pawula theorem check: D4 should be ~0 for true Langevin process
    pawula_ratio = np.mean(np.abs(D4)) / (np.mean(np.abs(D2)) + 1e-12)

    # Fit drift as polynomial: D1(x) ≈ a0 + a1*x + a2*x^2 + a3*x^3
    if len(bin_centers) > 4:
        drift_coeffs = np.polyfit(bin_centers - np.mean(bin_centers), D1, min(3, len(bin_centers) - 1))
    else:
        drift_coeffs = []

    # Effective potential: V(x) = -∫D1(x)dx, approximated
    if len(D1) > 1:
        V_eff = -np.cumsum(D1) * (bin_centers[1] - bin_centers[0]) if len(bin_centers) > 1 else D1
    else:
        V_eff = np.array([0.0])

    # Detect potential wells (minima of V_eff)
    wells = []
    for i in range(1, len(V_eff) - 1):
        if V_eff[i] < V_eff[i - 1] and V_eff[i] < V_eff[i + 1]:
            wells.append({"position": float(bin_centers[i]), "depth": float(V_eff[i])})

    return {
        "bin_centers": bin_centers.tolist(),
        "D1_drift": D1.tolist(),
        "D2_diffusion": D2.tolist(),
        "D4_fourth": D4.tolist(),
        "counts_per_bin": [int(c) for c in counts],
        "drift_polynomial_coeffs": [float(c) for c in drift_coeffs] if len(drift_coeffs) > 0 else [],
        "effective_potential": V_eff.tolist(),
        "potential_wells": wells,
        "pawula_ratio": float(pawula_ratio),
        "is_langevin_valid": bool(pawula_ratio < 0.1),
        "diagnostics": {
            "total_points": len(x),
            "bins_used": len(bin_centers),
            "mean_D1": float(np.mean(D1)),
            "mean_D2": float(np.mean(D2)),
            "D2_variation": float(np.std(D2) / (np.mean(D2) + 1e-12)),
        },
    }


# ═══════════════════════════════════════════════════
# HURST EXPONENT
# ═══════════════════════════════════════════════════

def _anis_lloyd_expected_rs(n: int) -> float:
    """
    Anis-Lloyd (1976) correction: expected R/S for an iid Gaussian
    series of length n.  Subtracting this removes the well-known
    upward bias of classic R/S in finite samples.
    """
    if n < 2:
        return 0.0
    # E[R/S] ≈ (Γ((n-1)/2) / (√π · Γ(n/2))) · Σ_{i=1}^{n-1} √((n-i)/i)
    # For large n the Gamma ratio ≈ √(2/π) · 1/√n, so we use the
    # exact sum but approximate the Gamma ratio via Stirling when n>300.
    from math import lgamma, sqrt, exp, pi
    sum_term = sum(sqrt((n - i) / i) for i in range(1, n))
    if n <= 300:
        gamma_ratio = exp(lgamma((n - 1) / 2) - lgamma(n / 2)) / sqrt(pi)
    else:
        # Stirling approximation for the ratio
        gamma_ratio = sqrt(2.0 / (pi * n))
    return gamma_ratio * sum_term


def hurst_exponent(
    x: np.ndarray,
    min_window: int = 10,
    max_window: Optional[int] = None,
    as_returns: bool = False,
) -> dict:
    """
    Compute Hurst exponent via bias-corrected R/S analysis.

    H < 0.5: anti-persistent (mean-reverting)
    H = 0.5: random walk
    H > 0.5: persistent (trending)

    Parameters
    ----------
    x : array-like
        Price series (will be converted to log-returns internally)
        or a returns series if *as_returns=True*.
    min_window : int
        Smallest sub-window for R/S calculation.
    max_window : int | None
        Largest sub-window (default: N // 4).
    as_returns : bool
        If True, treat *x* as a returns series directly (skip
        the log-return conversion).
    """
    x = np.asarray(x, dtype=float)

    # ── Convert prices → log-returns ──────────────────────────────
    if not as_returns:
        x = np.diff(np.log(x + 1e-12))

    N = len(x)
    if N < 2 * min_window:
        return {"hurst": 0.5, "confidence": 0.0, "error": "insufficient_data"}

    if max_window is None:
        max_window = N // 4

    # Generate window sizes (log-spaced)
    window_sizes = np.unique(
        np.logspace(np.log10(min_window), np.log10(max_window), 30).astype(int)
    )
    window_sizes = window_sizes[(window_sizes >= min_window) & (window_sizes <= max_window)]

    rs_values = []
    valid_windows = []

    for w in window_sizes:
        n_windows = N // w
        if n_windows < 2:
            continue

        rs_list = []
        for i in range(n_windows):
            segment = x[i * w : (i + 1) * w]
            mean = np.mean(segment)
            devs = np.cumsum(segment - mean)
            R = np.max(devs) - np.min(devs)
            S = np.std(segment, ddof=1)
            if S > 1e-15:
                rs_list.append(R / S)

        if len(rs_list) >= 2:
            # Anis-Lloyd bias correction: subtract expected R/S under iid null
            expected_rs = _anis_lloyd_expected_rs(w)
            mean_rs = np.mean(rs_list)
            corrected_rs = max(mean_rs - expected_rs + expected_rs * 0.5 ** 0.0,
                               mean_rs)  # keep raw if correction overshoots
            # Actually, standard approach: regress the *corrected* R/S
            # corrected = (R/S)_observed  /  E[R/S]_iid   (ratio method)
            # Then H is the slope, calibrated so that random walk → 0.5
            rs_values.append(mean_rs)
            valid_windows.append(w)

    if len(valid_windows) < 3:
        return {"hurst": 0.5, "confidence": 0.0, "error": "insufficient_data"}

    log_w = np.log(valid_windows)
    log_rs = np.log(rs_values)

    # ── Anis-Lloyd correction via expected-RS regression ──────────
    # Expected log(E[R/S]) for each window under H₀ of iid Gaussian
    log_rs_expected = np.array(
        [np.log(max(_anis_lloyd_expected_rs(w), 1e-12)) for w in valid_windows]
    )
    # Corrected log R/S: shift so that a true random walk yields slope≈0.5
    log_rs_corrected = log_rs - log_rs_expected + 0.5 * log_w

    slope, intercept, r_value, p_value, std_err = stats.linregress(
        log_w, log_rs_corrected
    )

    # Clamp to theoretically valid range [0, 1]
    hurst = float(np.clip(slope, 0.0, 1.0))

    regime = (
        "anti_persistent" if hurst < 0.45
        else ("persistent" if hurst > 0.55 else "random_walk")
    )

    return {
        "hurst": round(hurst, 4),
        "r_squared": round(float(r_value**2), 4),
        "std_error": round(float(std_err), 4),
        "p_value": round(float(p_value), 6),
        "regime": regime,
        "window_sizes": [int(w) for w in valid_windows],
        "log_rs_values": [round(float(v), 4) for v in log_rs],
        "note": "Input auto-converted to log-returns; Anis-Lloyd bias correction applied.",
    }


# ═══════════════════════════════════════════════════
# LYAPUNOV EXPONENT (Rosenstein method)
# ═══════════════════════════════════════════════════

def lyapunov_exponent(
    x: np.ndarray,
    embedding_dim: int = 5,
    tau: int = 1,
    min_separation: int = 10,
    max_iter: int = 50,
    dt: float = 1.0,
) -> dict:
    """
    Estimate maximum Lyapunov exponent using Rosenstein's method.
    λ > 0: chaotic (sensitive to initial conditions)
    λ ≈ 0: marginally stable
    λ < 0: stable (converging trajectories)
    """
    x = np.asarray(x, dtype=float)
    N = len(x)

    # Phase space reconstruction (delay embedding)
    M = N - (embedding_dim - 1) * tau
    if M < 2 * min_separation:
        return {"lyapunov": 0.0, "error": "insufficient_data"}

    embedded = np.zeros((M, embedding_dim))
    for i in range(embedding_dim):
        embedded[:, i] = x[i * tau : i * tau + M]

    # Find nearest neighbors (excluding temporal neighbors)
    divergence = np.zeros(min(max_iter, M // 2))
    count = np.zeros_like(divergence)

    for i in range(M - max_iter):
        # Find nearest neighbor
        dists = np.sqrt(np.sum((embedded - embedded[i]) ** 2, axis=1))
        # Exclude temporal neighbors
        for j in range(max(0, i - min_separation), min(M, i + min_separation + 1)):
            dists[j] = np.inf

        nn = np.argmin(dists)
        if dists[nn] == np.inf or dists[nn] == 0:
            continue

        # Track divergence
        for k in range(min(max_iter, M - max(i, nn) - 1)):
            d = np.sqrt(np.sum((embedded[i + k] - embedded[nn + k]) ** 2))
            if d > 0:
                divergence[k] += np.log(d)
                count[k] += 1

    # Average log divergence
    valid = count > 0
    if valid.sum() < 5:
        return {"lyapunov": 0.0, "error": "insufficient_neighbors"}

    avg_divergence = np.zeros_like(divergence)
    avg_divergence[valid] = divergence[valid] / count[valid]

    # Linear fit to estimate λ
    t_vals = np.arange(len(avg_divergence))[valid][:20] * dt
    d_vals = avg_divergence[valid][:20]

    if len(t_vals) < 3:
        return {"lyapunov": 0.0, "error": "insufficient_fit_points"}

    slope, intercept, r_value, p_value, std_err = stats.linregress(t_vals, d_vals)

    regime = "chaotic" if slope > 0.01 else ("stable" if slope < -0.01 else "marginally_stable")

    return {
        "lyapunov": round(float(slope), 6),
        "r_squared": round(float(r_value**2), 4),
        "std_error": round(float(std_err), 6),
        "regime": regime,
        "embedding_dim": embedding_dim,
        "delay_tau": tau,
        "divergence_curve": [round(float(d), 4) for d in avg_divergence[valid][:20]],
    }


# ═══════════════════════════════════════════════════
# RECURRENCE QUANTIFICATION ANALYSIS (RQA)
# ═══════════════════════════════════════════════════

def rqa_analysis(
    x: np.ndarray,
    embedding_dim: int = 3,
    tau: int = 1,
    target_rr: float = 0.01,
    threshold_pct: Optional[float] = None,
    theiler_window: Optional[int] = None,
    v_min: int = 2,
    max_points: int = 2000,
) -> dict:
    """
    Compute RQA measures from recurrence plot.
    Key indicators: RR, DET, LAM, L_max, ENTR, TT.
    LAM (laminarity) is the most sensitive pre-crash indicator.

    Fixes applied (vs original):
      1. Adaptive threshold targeting a specific recurrence rate (RR).
         Literature recommends RR ∈ [1%, 5%]; default 1%.
      2. Theiler window to exclude temporally correlated neighbors from the
         recurrence plot. Without it, the embedding overlap (tau=1 shares
         dim-1 components) inflates LAM to ~1.0 for any series.
      3. LAM denominator uses full-matrix recurrence_sum (vertical scan
         covers all columns), not recurrence_sum/2.

    Parameters:
      target_rr:      Target recurrence rate (default 0.01 = 1%).
      threshold_pct:  If provided, overrides adaptive mode and uses this
                      percentile of the distance distribution (legacy).
      theiler_window: Exclude pairs within this many time steps from the
                      diagonal. Default: tau*(embedding_dim-1) + 1.
      v_min:          Minimum vertical/diagonal line length (default 2).
    """
    x = np.asarray(x, dtype=float)

    # Subsample if too large
    if len(x) > max_points:
        step = len(x) // max_points
        x = x[::step]

    N = len(x)
    M = N - (embedding_dim - 1) * tau
    if M < 20:
        return {"error": "insufficient_data"}

    # Phase space reconstruction
    embedded = np.zeros((M, embedding_dim))
    for i in range(embedding_dim):
        embedded[:, i] = x[i * tau : i * tau + M]

    # Distance matrix
    dists = squareform(pdist(embedded, metric="euclidean"))

    # ── Theiler window: mask out temporally close pairs ──
    if theiler_window is None:
        theiler_window = tau * (embedding_dim - 1) + 1
    idx = np.arange(M)
    theiler_mask = np.abs(idx[:, None] - idx[None, :]) > theiler_window

    # ── Threshold selection (computed from distances OUTSIDE Theiler window) ──
    valid_dists = dists[theiler_mask]
    if threshold_pct is not None:
        # Legacy mode: percentile of valid distances
        vd = valid_dists[valid_dists > 0]
        eps = np.percentile(vd, threshold_pct) if len(vd) > 0 else 1.0
        threshold_mode = "fixed_percentile"
    else:
        # Adaptive mode: percentile that yields the target RR
        vd = valid_dists[valid_dists > 0]
        adaptive_pct = target_rr * 100.0
        eps = np.percentile(vd, adaptive_pct) if len(vd) > 0 else 1.0
        threshold_mode = "adaptive_rr"

    # ── Build recurrence matrix ──
    recurrence = (dists <= eps).astype(int)
    np.fill_diagonal(recurrence, 0)           # Remove line of identity
    recurrence[~theiler_mask] = 0             # Remove Theiler corridor

    recurrence_sum = recurrence.sum()
    total_valid = int(theiler_mask.sum()) - M  # off-diagonal outside Theiler
    RR = recurrence_sum / total_valid if total_valid > 0 else 0

    # ── Diagonal line analysis (DET, L_max, ENTR) ──
    # Upper-triangle diagonals only (k > theiler_window to stay outside Theiler)
    diag_lengths = []
    for k in range(theiler_window + 1, M):
        diag = np.diag(recurrence, k)
        length = 0
        for val in diag:
            if val:
                length += 1
            else:
                if length >= v_min:
                    diag_lengths.append(length)
                length = 0
        if length >= v_min:
            diag_lengths.append(length)

    # Denominator for DET: recurrent points in upper triangle outside Theiler
    upper_recurrence = 0
    for k in range(theiler_window + 1, M):
        upper_recurrence += int(np.diag(recurrence, k).sum())

    if diag_lengths:
        DET = sum(diag_lengths) / (upper_recurrence + 1e-12)
        L_max = max(diag_lengths)
        L_mean = np.mean(diag_lengths)
        # Shannon entropy of line length distribution
        hist, _ = np.histogram(diag_lengths, bins=range(v_min, max(diag_lengths) + 2))
        hist = hist[hist > 0]
        p = hist / hist.sum()
        ENTR = -np.sum(p * np.log(p))
    else:
        DET, L_max, L_mean, ENTR = 0, 0, 0, 0

    # ── Vertical line analysis (LAM, TT) ──
    # Scans ALL columns of the full recurrence matrix (Theiler corridor
    # already zeroed out).  Denominator = recurrence_sum (full matrix).
    vert_lengths = []
    for col in range(M):
        length = 0
        for row in range(M):
            if recurrence[row, col]:
                length += 1
            else:
                if length >= v_min:
                    vert_lengths.append(length)
                length = 0
        if length >= v_min:
            vert_lengths.append(length)

    if vert_lengths:
        LAM = sum(vert_lengths) / (recurrence_sum + 1e-12)
        TT = np.mean(vert_lengths)  # Trapping time
    else:
        LAM, TT = 0, 0

    return {
        "recurrence_rate": round(float(RR), 6),
        "determinism": round(float(min(DET, 1.0)), 4),
        "laminarity": round(float(min(LAM, 1.0)), 4),
        "max_diagonal_line": int(L_max),
        "mean_diagonal_line": round(float(L_mean), 2),
        "entropy": round(float(ENTR), 4),
        "trapping_time": round(float(TT), 2),
        "embedding_dim": embedding_dim,
        "tau": tau,
        "target_rr": target_rr,
        "threshold_mode": threshold_mode,
        "theiler_window": theiler_window,
        "actual_rr": round(float(RR), 6),
        "epsilon": round(float(eps), 8),
        "matrix_size": M,
        "interpretation": {
            "high_LAM": "System approaching laminar (frozen) state — pre-crash signal",
            "high_DET": "Deterministic structure present — non-random dynamics",
            "high_ENTR": "Complex diagonal structure — rich dynamics",
            "high_TT": "Long trapping times — system stuck in states",
        },
    }


# ═══════════════════════════════════════════════════
# VPIN (Volume-synchronized Probability of Informed Trading)
# ═══════════════════════════════════════════════════

def compute_vpin(
    trades: list[dict],
    n_buckets: int = 50,
    window: int = 50,
) -> dict:
    """
    Compute VPIN from aggTrades using isBuyerMaker for accurate buy/sell classification.
    VPIN measures order flow toxicity — probability of adverse selection.

    trades:    list of dicts with keys 'qty' and 'is_buyer_maker'.
               is_buyer_maker=True → sell (taker sold), False → buy (taker bought).
    n_buckets: rolling window size (number of volume buckets averaged per VPIN value).
    """
    if not trades:
        return {"vpin": 0.0, "error": "no_trade_data"}

    # Extract per-trade buy/sell qty
    trade_qty = np.array([t["qty"] for t in trades], dtype=float)
    # is_buyer_maker=True means the maker's side was buy → taker sold → it's a SELL
    is_sell = np.array([t["is_buyer_maker"] for t in trades], dtype=bool)
    trade_buy = np.where(~is_sell, trade_qty, 0.0)
    trade_sell = np.where(is_sell, trade_qty, 0.0)

    total_volume = trade_qty.sum()
    if total_volume <= 0:
        return {"vpin": 0.0, "error": "no_volume_data"}

    # Bucket size: total volume / desired number of buckets.
    # We want many more buckets than the window to get a VPIN time series.
    # Target: ~(len(trades)/5) buckets — each bucket ≈ 5 trades on average.
    target_n = max(len(trades) // 5, window * 4)
    bucket_size = total_volume / target_n
    if bucket_size <= 0:
        return {"vpin": 0.0, "error": "bucket_size_zero"}

    # Fill volume buckets from individual trades
    buy_volumes = []
    sell_volumes = []
    cum_vol = 0.0
    bucket_buy = 0.0
    bucket_sell = 0.0

    for i in range(len(trade_qty)):
        remaining_buy = trade_buy[i]
        remaining_sell = trade_sell[i]
        remaining = trade_qty[i]

        while remaining > 1e-12:
            space = bucket_size - cum_vol
            fill = min(remaining, space)
            frac = fill / trade_qty[i] if trade_qty[i] > 0 else 0
            bucket_buy += remaining_buy * frac
            bucket_sell += remaining_sell * frac
            cum_vol += fill
            remaining -= fill
            remaining_buy -= trade_buy[i] * frac
            remaining_sell -= trade_sell[i] * frac

            if cum_vol >= bucket_size * 0.999:
                buy_volumes.append(bucket_buy)
                sell_volumes.append(bucket_sell)
                cum_vol = 0.0
                bucket_buy = 0.0
                bucket_sell = 0.0

    if len(buy_volumes) < window:
        return {"vpin": 0.0, "error": "insufficient_buckets",
                "detail": f"got {len(buy_volumes)} buckets, need {window}"}

    buy_arr = np.array(buy_volumes)
    sell_arr = np.array(sell_volumes)

    # Order imbalance per bucket
    OI = np.abs(buy_arr - sell_arr)

    # VPIN = rolling mean of |OI| / bucket_size
    vpin_series = []
    for i in range(window, len(OI) + 1):
        vpin_val = np.mean(OI[i - window : i]) / bucket_size
        vpin_series.append(float(vpin_val))

    current_vpin = vpin_series[-1] if vpin_series else 0
    mean_vpin = float(np.mean(vpin_series)) if vpin_series else 0
    std_vpin = float(np.std(vpin_series)) if vpin_series else 0
    z_score = (current_vpin - mean_vpin) / std_vpin if std_vpin > 1e-12 else 0.0

    return {
        "vpin_current": round(float(current_vpin), 4),
        "vpin_mean": round(float(mean_vpin), 4),
        "vpin_std": round(float(std_vpin), 4),
        "vpin_z_score": round(float(z_score), 2),
        "vpin_percentile": round(float(stats.percentileofscore(vpin_series, current_vpin)), 1) if vpin_series else 0.0,
        "toxicity_level": "HIGH" if z_score > 2 else ("ELEVATED" if z_score > 1 else "NORMAL"),
        "n_trades": len(trades),
        "n_buckets_total": len(buy_volumes),
        "n_vpin_observations": len(vpin_series),
        "bucket_size": round(bucket_size, 2),
        "vpin_series_last20": [round(v, 4) for v in vpin_series[-20:]],
        "source": "aggTrades_isBuyerMaker",
    }


# ═══════════════════════════════════════════════════
# KYLE'S LAMBDA (Price Impact Estimator)
# ═══════════════════════════════════════════════════

def kyles_lambda(
    price_changes: np.ndarray,
    signed_volumes: np.ndarray,
) -> dict:
    """
    Estimate Kyle's lambda (price impact coefficient).
    ΔP = λ * SignedVolume + ε
    Higher λ = less liquid market, more adverse selection.
    """
    price_changes = np.asarray(price_changes, dtype=float)
    signed_volumes = np.asarray(signed_volumes, dtype=float)

    # Remove zero volumes
    mask = signed_volumes != 0
    if mask.sum() < 10:
        return {"lambda": 0.0, "error": "insufficient_data"}

    dp = price_changes[mask]
    sv = signed_volumes[mask]

    slope, intercept, r_value, p_value, std_err = stats.linregress(sv, dp)

    # Rolling lambda (last 50 points)
    rolling_lambdas = []
    w = min(50, len(dp) // 3)
    for i in range(w, len(dp)):
        s, _, _, _, _ = stats.linregress(sv[i - w : i], dp[i - w : i])
        rolling_lambdas.append(float(s))

    return {
        "kyle_lambda": round(float(slope), 8),
        "r_squared": round(float(r_value**2), 4),
        "p_value": round(float(p_value), 6),
        "std_error": round(float(std_err), 8),
        "lambda_trend": "INCREASING" if len(rolling_lambdas) > 5 and rolling_lambdas[-1] > np.mean(rolling_lambdas[:5]) else "STABLE",
        "rolling_lambda_last10": [round(v, 8) for v in rolling_lambdas[-10:]],
    }


# ═══════════════════════════════════════════════════
# JAM REGIME DETECTION
# ═══════════════════════════════════════════════════

def jam_regime_analysis(
    closes: np.ndarray,
    volumes: np.ndarray,
    taker_buy_volumes: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    window: int = 20,
) -> dict:
    """
    Full JAM regime analysis.
    Classifies between:
    - Régimen A: Sustained impulse (volume>2x, delta>60%, retention>70%, absorption<0.95, speed<180min)
    - Régimen B: Failed pump (fails one or more criteria)
    - Neutral: No significant impulse detected

    Uses Langevin physics analogies:
    - Volume surge = external force F_ext
    - Delta (buy-sell ratio) = force directionality
    - Retention = damping γ (low retention = high damping, energy dissipation)
    - Absorption = restoring force κ (high absorption = mean reversion)
    """
    closes = np.asarray(closes, dtype=float)
    volumes = np.asarray(volumes, dtype=float)
    taker_buy_volumes = np.asarray(taker_buy_volumes, dtype=float)
    highs = np.asarray(highs, dtype=float)
    lows = np.asarray(lows, dtype=float)

    n = len(closes)
    if n < window * 2:
        return {"error": "insufficient_data", "min_required": window * 2}

    # Volume baseline (rolling mean)
    vol_baseline = np.convolve(volumes, np.ones(window) / window, mode="valid")
    # Pad to align
    pad = n - len(vol_baseline)
    vol_baseline = np.concatenate([np.full(pad, vol_baseline[0]), vol_baseline])

    # Volume ratio
    vol_ratio = volumes / (vol_baseline + 1e-12)

    # Delta (taker buy ratio)
    delta = taker_buy_volumes / (volumes + 1e-12)

    # Returns
    returns = np.diff(closes) / closes[:-1]
    returns = np.concatenate([[0], returns])

    # Retention: how much of the move is kept (rolling)
    retention_series = []
    for i in range(window, n):
        segment = closes[i - window : i + 1]
        max_move = np.max(segment) - segment[0]
        final_move = segment[-1] - segment[0]
        if abs(max_move) > 1e-12:
            retention_series.append(final_move / max_move)
        else:
            retention_series.append(0)

    retention_series = np.array(retention_series)

    # Absorption: ratio of volume at resistance/support levels
    absorption_series = []
    for i in range(window, n):
        high_zone = highs[i - window : i + 1]
        vol_zone = volumes[i - window : i + 1]
        max_high = np.max(high_zone)
        near_top = high_zone > max_high * 0.995
        if near_top.sum() > 0 and vol_zone.sum() > 0:
            absorption_series.append(vol_zone[near_top].sum() / vol_zone.sum())
        else:
            absorption_series.append(0)

    absorption_series = np.array(absorption_series)

    # Current state (last window)
    current = {
        "vol_ratio": round(float(vol_ratio[-1]), 2),
        "vol_ratio_mean_last5": round(float(np.mean(vol_ratio[-5:])), 2),
        "delta": round(float(delta[-1]), 4),
        "delta_mean_last5": round(float(np.mean(delta[-5:])), 4),
        "retention": round(float(retention_series[-1]), 4) if len(retention_series) > 0 else 0,
        "absorption": round(float(absorption_series[-1]), 4) if len(absorption_series) > 0 else 0,
    }

    # Regime classification
    vol_trigger = current["vol_ratio_mean_last5"] > 2.0
    delta_strong = abs(current["delta_mean_last5"] - 0.5) > 0.1  # delta > 60% or < 40%
    retention_high = current["retention"] > 0.70
    absorption_low = current["absorption"] < 0.95

    if vol_trigger:
        if delta_strong and retention_high and absorption_low:
            regime = "A"
            regime_desc = "RÉGIMEN A — Impulso sostenido. F_ext dominante, γ bajo, κ no restaura."
        else:
            regime = "B"
            failures = []
            if not delta_strong:
                failures.append("delta_weak (fuerza sin dirección)")
            if not retention_high:
                failures.append("retention_low (γ alto, energía disipada)")
            if not absorption_low:
                failures.append("absorption_high (κ restaura, mean-reversion)")
            regime_desc = f"RÉGIMEN B — Pump fallido. Fallas: {', '.join(failures)}"
    else:
        regime = "NEUTRAL"
        regime_desc = "NEUTRAL — Sin impulso significativo. F_ext < umbral."

    # Langevin parameter estimation
    # γ_eff ∝ 1/retention (high retention = low damping)
    gamma_eff = 1.0 / (current["retention"] + 0.01)
    # κ_eff ∝ absorption (high absorption = strong restoring)
    kappa_eff = current["absorption"]
    # F_ext ∝ vol_ratio * |delta - 0.5|
    f_ext = current["vol_ratio"] * abs(current["delta"] - 0.5) * 2

    return {
        "regime": regime,
        "regime_description": regime_desc,
        "current_state": current,
        "criteria": {
            "volume_trigger": vol_trigger,
            "delta_strong": delta_strong,
            "retention_high": retention_high,
            "absorption_low": absorption_low,
        },
        "langevin_params": {
            "gamma_eff_damping": round(float(gamma_eff), 4),
            "kappa_eff_restoring": round(float(kappa_eff), 4),
            "F_ext_force": round(float(f_ext), 4),
            "interpretation": {
                "gamma": "γ_eff = 1/retention. Alto → energía se disipa rápido (pump fallido)",
                "kappa": "κ_eff ∝ absorption. Alto → fuerza restauradora domina (mean-reversion)",
                "F_ext": "F_ext ∝ vol_ratio × |delta|. Fuerza externa aplicada al sistema",
            },
        },
        "vol_ratio_series_last20": [round(float(v), 2) for v in vol_ratio[-20:]],
        "delta_series_last20": [round(float(v), 4) for v in delta[-20:]],
    }


# ═══════════════════════════════════════════════════
# FULL PSI-JAM PIPELINE
# ═══════════════════════════════════════════════════

def full_psi_jam_analysis(
    closes: np.ndarray,
    volumes: np.ndarray,
    taker_buy_volumes: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    trades: list[dict] | None = None,
) -> dict:
    """
    Run the complete Ψ-jam analysis pipeline:
    1. Kramers-Moyal coefficient extraction
    2. Hurst exponent
    3. Lyapunov exponent
    4. RQA analysis
    5. JAM regime detection
    6. VPIN flow toxicity (from aggTrades if available)
    """
    log_returns = np.diff(np.log(closes + 1e-12))

    results = {}

    # 1. Kramers-Moyal
    try:
        results["kramers_moyal"] = kramers_moyal_coefficients(log_returns)
    except Exception as e:
        results["kramers_moyal"] = {"error": str(e)}

    # 2. Hurst
    try:
        results["hurst"] = hurst_exponent(closes)
    except Exception as e:
        results["hurst"] = {"error": str(e)}

    # 3. Lyapunov
    try:
        results["lyapunov"] = lyapunov_exponent(log_returns)
    except Exception as e:
        results["lyapunov"] = {"error": str(e)}

    # 4. RQA
    try:
        results["rqa"] = rqa_analysis(log_returns)
    except Exception as e:
        results["rqa"] = {"error": str(e)}

    # 5. JAM Regime
    try:
        results["jam_regime"] = jam_regime_analysis(
            closes, volumes, taker_buy_volumes, highs, lows
        )
    except Exception as e:
        results["jam_regime"] = {"error": str(e)}

    # 6. VPIN (from aggTrades if available, otherwise skip)
    try:
        if trades:
            results["vpin"] = compute_vpin(trades)
        else:
            results["vpin"] = {"vpin_current": 0.0, "error": "no_trades_provided",
                               "detail": "Use analyze_vpin for proper VPIN with aggTrades"}
    except Exception as e:
        results["vpin"] = {"error": str(e)}

    # Composite risk score
    try:
        risk_signals = []
        if "hurst" in results and "hurst" in results["hurst"]:
            h = results["hurst"]["hurst"]
            if h < 0.4:
                risk_signals.append(("hurst_anti_persistent", 0.7))
            elif h > 0.7:
                risk_signals.append(("hurst_strongly_persistent", 0.3))
        if "lyapunov" in results and "lyapunov" in results["lyapunov"]:
            lam = results["lyapunov"]["lyapunov"]
            if lam > 0.05:
                risk_signals.append(("lyapunov_chaotic", 0.8))
        if "rqa" in results and "laminarity" in results["rqa"]:
            lam_rqa = results["rqa"]["laminarity"]
            if lam_rqa > 0.7:
                risk_signals.append(("laminarity_high", 0.9))
        if "vpin" in results and "vpin_z_score" in results["vpin"]:
            z = results["vpin"]["vpin_z_score"]
            if z > 2:
                risk_signals.append(("vpin_toxic", 0.85))
        if "jam_regime" in results and results["jam_regime"].get("regime") == "B":
            risk_signals.append(("regime_B_failed_pump", 0.75))

        composite = np.mean([s[1] for s in risk_signals]) if risk_signals else 0.2
        results["composite_risk"] = {
            "score": round(float(composite), 3),
            "signals": risk_signals,
            "level": "CRITICAL" if composite > 0.7 else ("HIGH" if composite > 0.5 else ("MODERATE" if composite > 0.3 else "LOW")),
        }
    except Exception as e:
        results["composite_risk"] = {"error": str(e)}

    return results


# ═══════════════════════════════════════════════════
# LIQUIDATION CLUSTERS ANALYSIS
# ═══════════════════════════════════════════════════

def calculate_liquidation_clusters(
    current_price: float,
    klines: list[dict],
    oi_data: dict,
    oi_history: Optional[list[dict]] = None,
    funding_rate: float = 0.0,
    leverage_levels: Optional[list[int]] = None,
) -> dict:
    """
    Calculate liquidation price clusters based on price history and OI.
    
    Estimates where liquidations might be concentrated by analyzing:
    1. Price levels where positions were likely opened (high volume zones)
    2. Common leverage levels (2x, 3x, 5x, 10x, 20x, 25x, 50x, 100x)
    3. Current OI distribution
    
    Liquidation formulas (cross margin, simplified):
    - LONG: liq_price ≈ entry_price * (1 - 1/leverage + fees)
    - SHORT: liq_price ≈ entry_price * (1 + 1/leverage + fees)
    
    Args:
        current_price: Current mark/last price
        klines: Historical OHLCV data (used to identify entry zones)
        oi_data: Current OI snapshot
        oi_history: Historical OI changes (optional)
        funding_rate: Current funding rate (affects position cost)
        leverage_levels: Custom leverage levels to analyze
        
    Returns:
        Dict with liquidation clusters for longs and shorts.
    """
    if leverage_levels is None:
        leverage_levels = [2, 3, 5, 10, 20, 25, 50, 75, 100]
    
    # Extract price data from klines
    closes = np.array([k["close"] for k in klines])
    volumes = np.array([k.get("quote_volume", k.get("volume", 0)) for k in klines])
    highs = np.array([k["high"] for k in klines])
    lows = np.array([k["low"] for k in klines])
    
    # Current OI
    total_oi = oi_data.get("open_interest", 0)
    total_oi_value = oi_data.get("sum_open_interest_value", total_oi * current_price)
    
    # Find volume-weighted average price zones (likely entry zones)
    # Split price range into 20 buckets
    price_min = float(np.min(lows))
    price_max = float(np.max(highs))
    n_buckets = 20
    
    price_buckets = np.linspace(price_min, price_max, n_buckets + 1)
    bucket_volume = np.zeros(n_buckets)
    bucket_centers = np.zeros(n_buckets)
    
    for i in range(n_buckets):
        bucket_low = price_buckets[i]
        bucket_high = price_buckets[i + 1]
        bucket_centers[i] = (bucket_low + bucket_high) / 2
        
        # Sum volume where price passed through this bucket
        for j, (h, l, v) in enumerate(zip(highs, lows, volumes)):
            overlap = max(0, min(h, bucket_high) - max(l, bucket_low))
            candle_range = h - l if h > l else 1
            fraction = overlap / candle_range
            bucket_volume[i] += v * fraction
    
    # Normalize bucket volumes to get % of total activity
    total_vol = np.sum(bucket_volume)
    bucket_weights = bucket_volume / total_vol if total_vol > 0 else np.ones(n_buckets) / n_buckets
    
    # Identify high-activity zones (top 30% by volume)
    volume_threshold = np.percentile(bucket_volume, 70)
    high_activity_zones = [
        {
            "price_low": float(price_buckets[i]),
            "price_high": float(price_buckets[i + 1]),
            "center": float(bucket_centers[i]),
            "volume_pct": float(bucket_weights[i] * 100),
        }
        for i in range(n_buckets)
        if bucket_volume[i] >= volume_threshold
    ]
    
    # Calculate liquidation clusters for each leverage level
    # Assume positions entered at volume-weighted centers
    long_clusters = []
    short_clusters = []
    
    # Maintenance margin rate (varies by position size, using typical 0.5%)
    maint_margin_rate = 0.005
    
    for lev in leverage_levels:
        # For each high-activity entry zone, calculate liquidation levels
        for zone in high_activity_zones:
            entry_price = zone["center"]
            zone_weight = zone["volume_pct"] / 100
            
            # Estimated OI in this zone (proportional to volume)
            estimated_oi_usdt = total_oi_value * zone_weight
            
            # LONG liquidation: price drops
            # liq_price = entry * (1 - 1/leverage + maint_margin_rate)
            long_liq_price = entry_price * (1 - 1/lev + maint_margin_rate)
            
            # SHORT liquidation: price rises
            # liq_price = entry * (1 + 1/leverage - maint_margin_rate)
            short_liq_price = entry_price * (1 + 1/lev - maint_margin_rate)
            
            if long_liq_price > 0:  # Valid liquidation price
                long_clusters.append({
                    "leverage": lev,
                    "entry_zone": round(entry_price, 6),
                    "liquidation_price": round(long_liq_price, 6),
                    "distance_pct": round((current_price - long_liq_price) / current_price * 100, 2),
                    "estimated_oi_usdt": round(estimated_oi_usdt / len(leverage_levels), 2),
                    "zone_activity_pct": round(zone["volume_pct"], 2),
                })
            
            if short_liq_price > 0:
                short_clusters.append({
                    "leverage": lev,
                    "entry_zone": round(entry_price, 6),
                    "liquidation_price": round(short_liq_price, 6),
                    "distance_pct": round((short_liq_price - current_price) / current_price * 100, 2),
                    "estimated_oi_usdt": round(estimated_oi_usdt / len(leverage_levels), 2),
                    "zone_activity_pct": round(zone["volume_pct"], 2),
                })
    
    # Sort by distance to current price
    long_clusters.sort(key=lambda x: abs(x["distance_pct"]))
    short_clusters.sort(key=lambda x: abs(x["distance_pct"]))
    
    # Aggregate clusters by price zones (within 1% bands)
    def aggregate_clusters(clusters: list, band_pct: float = 1.0) -> list:
        """Group nearby liquidation levels into aggregate zones."""
        if not clusters:
            return []
        
        aggregated = []
        clusters_sorted = sorted(clusters, key=lambda x: x["liquidation_price"])
        
        current_band = None
        for c in clusters_sorted:
            liq = c["liquidation_price"]
            if current_band is None:
                current_band = {
                    "price_low": liq,
                    "price_high": liq,
                    "price_center": liq,
                    "leverages": [c["leverage"]],
                    "total_estimated_oi_usdt": c["estimated_oi_usdt"],
                    "count": 1,
                    "avg_distance_pct": c["distance_pct"],
                }
            elif abs(liq - current_band["price_center"]) / current_band["price_center"] * 100 <= band_pct:
                # Within band, merge
                current_band["price_high"] = max(current_band["price_high"], liq)
                current_band["price_low"] = min(current_band["price_low"], liq)
                current_band["price_center"] = (current_band["price_low"] + current_band["price_high"]) / 2
                if c["leverage"] not in current_band["leverages"]:
                    current_band["leverages"].append(c["leverage"])
                current_band["total_estimated_oi_usdt"] += c["estimated_oi_usdt"]
                current_band["count"] += 1
                current_band["avg_distance_pct"] = (
                    current_band["avg_distance_pct"] * (current_band["count"] - 1) + c["distance_pct"]
                ) / current_band["count"]
            else:
                # Start new band
                aggregated.append(current_band)
                current_band = {
                    "price_low": liq,
                    "price_high": liq,
                    "price_center": liq,
                    "leverages": [c["leverage"]],
                    "total_estimated_oi_usdt": c["estimated_oi_usdt"],
                    "count": 1,
                    "avg_distance_pct": c["distance_pct"],
                }
        
        if current_band:
            aggregated.append(current_band)
        
        # Round values
        for agg in aggregated:
            agg["price_low"] = round(agg["price_low"], 6)
            agg["price_high"] = round(agg["price_high"], 6)
            agg["price_center"] = round(agg["price_center"], 6)
            agg["total_estimated_oi_usdt"] = round(agg["total_estimated_oi_usdt"], 2)
            agg["avg_distance_pct"] = round(agg["avg_distance_pct"], 2)
        
        return sorted(aggregated, key=lambda x: abs(x["avg_distance_pct"]))
    
    long_aggregated = aggregate_clusters(long_clusters, band_pct=1.5)
    short_aggregated = aggregate_clusters(short_clusters, band_pct=1.5)
    
    # Calculate cascade risk zones (high density of liquidations)
    def find_cascade_zones(aggregated: list, side: str) -> list:
        """Identify zones where cascading liquidations are likely."""
        cascades = []
        for zone in aggregated[:10]:  # Top 10 nearest zones
            oi_density = zone["total_estimated_oi_usdt"]
            n_leverages = len(zone["leverages"])
            distance = abs(zone["avg_distance_pct"])
            
            # Risk = high OI + multiple leverage levels + close proximity
            cascade_risk = 0
            if oi_density > total_oi_value * 0.05:  # >5% of total OI
                cascade_risk += 30
            if n_leverages >= 3:  # Multiple leverage levels converge
                cascade_risk += 25
            if distance < 5:  # Within 5%
                cascade_risk += 25
            elif distance < 10:  # Within 10%
                cascade_risk += 15
            if 100 in zone["leverages"] or 75 in zone["leverages"]:
                cascade_risk += 20  # High leverage = quick liquidation
            
            risk_level = "CRITICAL" if cascade_risk >= 70 else (
                "HIGH" if cascade_risk >= 50 else (
                    "MODERATE" if cascade_risk >= 30 else "LOW"
                )
            )
            
            cascades.append({
                "price_zone": f"{zone['price_low']:.6f} - {zone['price_high']:.6f}",
                "center": zone["price_center"],
                "distance_pct": zone["avg_distance_pct"],
                "estimated_oi_usdt": zone["total_estimated_oi_usdt"],
                "leverages_affected": sorted(zone["leverages"]),
                "cascade_risk_score": cascade_risk,
                "risk_level": risk_level,
                "side": side,
            })
        
        return sorted(cascades, key=lambda x: x["cascade_risk_score"], reverse=True)
    
    long_cascades = find_cascade_zones(long_aggregated, "LONG")
    short_cascades = find_cascade_zones(short_aggregated, "SHORT")
    
    # Summary statistics
    nearest_long = long_clusters[0] if long_clusters else None
    nearest_short = short_clusters[0] if short_clusters else None
    
    return {
        "current_price": current_price,
        "total_oi": total_oi,
        "total_oi_value_usdt": total_oi_value,
        "funding_rate": funding_rate,
        "analysis_period": {
            "candles": len(klines),
            "price_range": {"low": float(price_min), "high": float(price_max)},
        },
        "leverage_levels_analyzed": leverage_levels,
        "high_activity_entry_zones": high_activity_zones,
        "long_liquidation_clusters": {
            "detail": long_clusters[:20],  # Top 20 nearest
            "aggregated_zones": long_aggregated[:10],  # Top 10 aggregated
            "cascade_risk_zones": long_cascades[:5],
            "nearest": nearest_long,
        },
        "short_liquidation_clusters": {
            "detail": short_clusters[:20],
            "aggregated_zones": short_aggregated[:10],
            "cascade_risk_zones": short_cascades[:5],
            "nearest": nearest_short,
        },
        "summary": {
            "long_liquidations_below": f"{nearest_long['liquidation_price']:.6f} ({nearest_long['distance_pct']:.1f}% away)" if nearest_long else "N/A",
            "short_liquidations_above": f"{nearest_short['liquidation_price']:.6f} ({nearest_short['distance_pct']:.1f}% away)" if nearest_short else "N/A",
            "critical_long_zones": len([c for c in long_cascades if c["risk_level"] == "CRITICAL"]),
            "critical_short_zones": len([c for c in short_cascades if c["risk_level"] == "CRITICAL"]),
        },
    }
