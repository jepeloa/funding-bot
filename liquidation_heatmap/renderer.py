"""
Renderer: HeatmapData → PNG/WebP  (Coinglass / Kingfisher style).

Visual design:
  • Dark purple–navy background
  • Viridis-style colormap  (purple → blue → green → yellow)
  • OHLC candlesticks overlaid on the heatmap
  • Colour-bar on the left showing the USD scale
  • Price axis on the right
  • Time axis on the bottom

Thread-safe: uses matplotlib Figure + Agg backend (no pyplot).
"""

import io
import logging
from datetime import datetime, timezone

import numpy as np
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.colors import LinearSegmentedColormap, LogNorm, PowerNorm, Normalize
from matplotlib.ticker import FuncFormatter, MaxNLocator
import matplotlib.dates as mdates
import matplotlib.colorbar as mcolorbar
from PIL import Image as PILImage

from .config import IMG_WIDTH, IMG_HEIGHT, WEBP_QUALITY
from .engine import HeatmapData

log = logging.getLogger("heatmap.renderer")

# ══════════════════════════════════════════════════════════════════
#  THEME  —  Coinglass palette
# ══════════════════════════════════════════════════════════════════

BG          = "#0b0e17"       # deep navy
CHART_BG    = "#0b0e17"
GRID_CLR    = "#1a1e2e"
TEXT_CLR    = "#7b8294"
TITLE_CLR   = "#e2e4ea"
CANDLE_UP   = "#26a69a"       # green
CANDLE_DN   = "#ef5350"       # red
CANDLE_WICK = "#888888"

# Coinglass-style colormap: deep purple → blue → teal → green → yellow
_CMAP_COLORS = [
    (0.00, "#0b0e17"),   # background (zero = invisible)
    (0.05, "#1a0a3a"),   # faint purple
    (0.12, "#2d1470"),   # dark purple
    (0.22, "#3b2094"),   # medium purple
    (0.32, "#3048b0"),   # blue-purple
    (0.42, "#1e6eaf"),   # blue
    (0.52, "#0d8e8e"),   # teal
    (0.62, "#10a858"),   # green
    (0.72, "#40c838"),   # bright green
    (0.82, "#80e020"),   # lime-green
    (0.90, "#c8d818"),   # yellow-green
    (0.96, "#f0e018"),   # yellow
    (1.00, "#ffee60"),   # bright yellow
]

_CMAP = LinearSegmentedColormap.from_list(
    "coinglass_liq",
    [(pos, col) for pos, col in _CMAP_COLORS],
    N=512,
)


# ══════════════════════════════════════════════════════════════════
#  PUBLIC API
# ══════════════════════════════════════════════════════════════════

def render_heatmap(data: HeatmapData,
                   width: int = IMG_WIDTH,
                   height: int = IMG_HEIGHT,
                   fmt: str = "webp") -> bytes:
    """Render a liquidation heatmap and return image bytes."""

    dpi = 100
    fig_w = width / dpi
    fig_h = height / dpi

    # ── Combine & prep heatmap data ──────────────────────────────
    # (n_time, n_price)  →  transpose to (n_price, n_time)
    combined = data.heatmap_long + data.heatmap_short
    if combined.size == 0 or combined.shape[0] < 2:
        # Fallback: return a minimal "no data" image
        combined = np.zeros((2, data.price_bins), dtype=np.float64)
    heatmap = combined.T.astype(np.float64)  # (price, time)

    # Gaussian smoothing: mostly horizontal for streak persistence,
    # light vertical for band smoothness  (Coinglass look)
    heatmap = _gaussian_blur(heatmap, sigma_y=1.5, sigma_x=6.0)

    # ── Time extent (matplotlib date-numbers) ────────────────────
    t0_dt = datetime.fromtimestamp(data.time_start, tz=timezone.utc)
    t1_dt = datetime.fromtimestamp(data.time_end, tz=timezone.utc)
    t0 = mdates.date2num(t0_dt)
    t1 = mdates.date2num(t1_dt)
    extent = [t0, t1, data.price_min, data.price_max]

    # ══════════════════════════════════════════════════════════════
    #  FIGURE LAYOUT  (Coinglass-style multi-panel)
    #
    #  ┌──────┬──────────────────────────┬───────┬──────┐
    #  │ cbar │       heatmap + candles  │ price │ liq  │
    #  │      │                          │ axis  │ prof │
    #  ├──────┼──────────────────────────┼───────┤      │
    #  │      │       volume bars        │       │      │
    #  └──────┴──────────────────────────┴───────┴──────┘
    # ══════════════════════════════════════════════════════════════

    fig = Figure(figsize=(fig_w, fig_h), dpi=dpi, facecolor=BG)
    canvas = FigureCanvas(fig)

    # Layout proportions
    left   = 0.065      # colorbar left
    right  = 0.83       # right edge of heatmap
    bottom = 0.14       # bottom of volume panel
    vol_h  = 0.08       # volume panel height
    gap    = 0.005      # gap between panels
    heat_b = bottom + vol_h + gap  # heatmap bottom
    heat_h = 0.88 - heat_b        # heatmap height
    prof_l = right + 0.005         # liq profile left
    prof_w = 0.06                  # liq profile width

    # Main heatmap axes
    ax = fig.add_axes([left, heat_b, right - left, heat_h])
    ax.set_facecolor(CHART_BG)

    # Volume axes (below heatmap, shared X)
    ax_vol = fig.add_axes([left, bottom, right - left, vol_h],
                          sharex=ax)
    ax_vol.set_facecolor(CHART_BG)

    # Liquidation profile axes (right of heatmap, shared Y)
    ax_prof = fig.add_axes([prof_l, heat_b, prof_w, heat_h],
                           sharey=ax)
    ax_prof.set_facecolor(CHART_BG)

    # ── Heatmap image ────────────────────────────────────────────
    positive = heatmap[heatmap > 0]
    if len(positive) > 50:
        vmin_raw = float(np.percentile(positive, 5))
        vmax_raw = float(np.percentile(positive, 99.5))
    else:
        vmin_raw = 1.0
        vmax_raw = 1000.0
    vmin_raw = max(vmin_raw, 1e-6)
    vmax_raw = max(vmax_raw, vmin_raw * 10)

    heatmap_masked = np.ma.masked_where(heatmap < vmin_raw * 0.3, heatmap)

    im = ax.imshow(
        heatmap_masked,
        aspect="auto",
        cmap=_CMAP,
        norm=PowerNorm(gamma=0.35, vmin=vmin_raw, vmax=vmax_raw),
        extent=extent,
        interpolation="bilinear",
        origin="lower",
        zorder=1,
    )

    # ── Candlestick overlay ──────────────────────────────────────
    if (data.opens is not None and len(data.opens) > 5
            and len(data.timestamps) == len(data.opens)):
        _draw_candlesticks(
            ax, data.timestamps, data.opens, data.highs_arr,
            data.lows_arr, data.closes, t0, t1, zorder=3,
        )

    # ── Mark price line + label ──────────────────────────────────
    ax.axhline(y=data.mark_price, color="#ffffff", linewidth=0.8,
               linestyle="--", alpha=0.45, zorder=4)
    # Price label badge on the right edge
    ax.annotate(
        f" {_fmt_price(data.mark_price)} ",
        xy=(t1, data.mark_price),
        xytext=(5, 0), textcoords="offset points",
        fontsize=7.5, fontweight="bold",
        color="#ffffff",
        bbox=dict(boxstyle="round,pad=0.15",
                  facecolor="#3b82f6", edgecolor="none", alpha=0.90),
        ha="left", va="center",
        clip_on=False, zorder=5,
        annotation_clip=False,
    )

    # ── Volume bars (bottom panel) ───────────────────────────────
    if data.volumes is not None and len(data.volumes) > 0:
        _draw_volume_bars(ax_vol, data, t0, t1)

    # ── Liquidation profile (right panel) ────────────────────────
    _draw_liq_profile(ax_prof, data)

    # ── Axes styling — Heatmap ───────────────────────────────────
    ax.xaxis.set_visible(False)   # time labels on volume panel
    ax.yaxis.tick_right()
    ax.yaxis.set_label_position("right")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: _fmt_price(v)))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=10, steps=[1, 2, 5, 10]))
    ax.tick_params(axis="y", colors=TEXT_CLR, labelsize=7.5,
                   length=3, width=0.5)
    ax.yaxis.grid(True, color=GRID_CLR, linewidth=0.3, alpha=0.4)
    for spine in ax.spines.values():
        spine.set_color(GRID_CLR)
        spine.set_linewidth(0.5)

    # ── Axes styling — Volume ────────────────────────────────────
    ax_vol.xaxis.set_major_formatter(mdates.DateFormatter("%d, %H:%M"))
    ax_vol.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax_vol.tick_params(axis="x", colors=TEXT_CLR, labelsize=7,
                       length=2, width=0.5, rotation=0)
    ax_vol.tick_params(axis="y", colors=TEXT_CLR, labelsize=6,
                       length=2, width=0.4)
    ax_vol.yaxis.tick_right()
    ax_vol.yaxis.set_major_formatter(FuncFormatter(lambda v, _: _fmt_usd(v)))
    ax_vol.yaxis.set_major_locator(MaxNLocator(nbins=3))
    for spine in ax_vol.spines.values():
        spine.set_color(GRID_CLR)
        spine.set_linewidth(0.5)
    ax_vol.set_ylabel("Vol", color=TEXT_CLR, fontsize=6.5,
                       rotation=0, labelpad=10, va="center")
    ax_vol.yaxis.set_label_position("left")

    # ── Axes styling — Liq Profile ───────────────────────────────
    ax_prof.xaxis.set_visible(False)
    ax_prof.yaxis.set_visible(False)
    for spine in ax_prof.spines.values():
        spine.set_color(GRID_CLR)
        spine.set_linewidth(0.5)

    # ── Colour bar (left side) ───────────────────────────────────
    cbar_ax = fig.add_axes([0.012, heat_b, 0.016, heat_h])
    cbar_ax.set_facecolor(BG)
    cb = mcolorbar.ColorbarBase(
        cbar_ax, cmap=_CMAP,
        norm=PowerNorm(gamma=0.35, vmin=vmin_raw, vmax=vmax_raw),
        orientation="vertical",
    )
    cb.ax.yaxis.set_ticks_position("left")
    cb.ax.tick_params(labelsize=6, colors=TEXT_CLR, length=2, width=0.4)
    cb.ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: _fmt_usd(v)))
    fig.text(0.012, heat_b + heat_h + 0.005, _fmt_usd(vmax_raw),
             color=TEXT_CLR, fontsize=6, va="bottom")

    # ── Title + info bar ─────────────────────────────────────────
    oi_str = _fmt_usd(data.oi_value)
    # Price change over period
    if data.opens is not None and len(data.opens) > 1:
        first_open = float(data.opens[0])
        last_close = float(data.closes[-1])
        pct_change = ((last_close - first_open) / first_open) * 100
        chg_color = CANDLE_UP if pct_change >= 0 else CANDLE_DN
        chg_str = f"{pct_change:+.2f}%"
    else:
        chg_str = "—"
        chg_color = TEXT_CLR

    # Total liquidation volume
    total_liq = float(np.sum(data.liq_long) + np.sum(data.liq_short))
    liq_str = _fmt_usd(total_liq)

    title = f"{data.symbol}  ·  Estimated Liquidation Heatmap"
    fig.text(left, 0.975, title, color=TITLE_CLR, fontsize=11,
             fontweight="bold", va="top", fontfamily="sans-serif")

    # Info line: OI · Mark · Change · Est. Liqs · Lookback · Render time
    info_parts = [
        f"OI: {oi_str}",
        f"Mark: {_fmt_price(data.mark_price)}",
    ]
    fig.text(left, 0.945, "   ·   ".join(info_parts),
             color=TEXT_CLR, fontsize=7.5, va="top")

    # Change badge
    fig.text(left + 0.27, 0.945, f"  {chg_str}  ",
             color=chg_color, fontsize=7.5, fontweight="bold", va="top",
             bbox=dict(boxstyle="round,pad=0.12",
                       facecolor=chg_color, edgecolor="none", alpha=0.15))

    # Second info line
    info2 = (f"Est. Liquidations: {liq_str}   ·   "
             f"Lookback: {data.lookback_hours}h   ·   "
             f"Rendered: {data.compute_time_ms:.0f}ms")
    fig.text(left, 0.92, info2, color=TEXT_CLR, fontsize=6.5, va="top")

    # ══════════════════════════════════════════════════════════════
    #  ENCODE
    # ══════════════════════════════════════════════════════════════
    canvas.draw()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=BG, edgecolor="none")
    buf.seek(0)

    if fmt == "webp":
        pil_img = PILImage.open(buf)
        out = io.BytesIO()
        pil_img.save(out, format="WEBP", quality=WEBP_QUALITY)
        out.seek(0)
        return out.getvalue()

    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════
#  VOLUME BARS  (bottom panel)
# ══════════════════════════════════════════════════════════════════

def _draw_volume_bars(ax_vol, data: HeatmapData, t0_mdate, t1_mdate):
    """Draw volume bars coloured by candle direction."""
    from matplotlib.patches import Rectangle
    from matplotlib.collections import PatchCollection

    n = len(data.timestamps)
    if n < 2:
        return

    mdates_arr = np.array([
        mdates.date2num(datetime.fromtimestamp(ts, tz=timezone.utc))
        for ts in data.timestamps
    ])
    volumes = data.volumes
    opens = data.opens
    closes = data.closes

    # Resample to match candlestick resolution (~72)
    target = 72
    if n > target:
        factor = max(round(n / target), 2)
        trim = (n // factor) * factor
        mdates_arr = mdates_arr[:trim].reshape(-1, factor)[:, factor // 2]
        volumes = volumes[:trim].reshape(-1, factor).sum(axis=1)
        opens = opens[:trim].reshape(-1, factor)[:, 0]
        closes = closes[:trim].reshape(-1, factor)[:, -1]
        n = len(mdates_arr)

    bar_w = np.median(np.diff(mdates_arr)) * 0.70 if n > 1 else (t1_mdate - t0_mdate) * 0.01

    patches_up = []
    patches_dn = []
    for i in range(n):
        x = float(mdates_arr[i])
        v = float(volumes[i])
        rect = Rectangle((x - bar_w / 2, 0), bar_w, v)
        if closes[i] >= opens[i]:
            patches_up.append(rect)
        else:
            patches_dn.append(rect)

    if patches_up:
        pc = PatchCollection(patches_up, facecolors=CANDLE_UP,
                             edgecolors="none", alpha=0.65, zorder=2)
        ax_vol.add_collection(pc)
    if patches_dn:
        pc = PatchCollection(patches_dn, facecolors=CANDLE_DN,
                             edgecolors="none", alpha=0.65, zorder=2)
        ax_vol.add_collection(pc)

    ax_vol.set_xlim(t0_mdate, t1_mdate)
    ax_vol.set_ylim(0, float(volumes.max()) * 1.15)


# ══════════════════════════════════════════════════════════════════
#  LIQUIDATION PROFILE  (right panel — horizontal bars)
# ══════════════════════════════════════════════════════════════════

def _draw_liq_profile(ax_prof, data: HeatmapData):
    """Draw horizontal bar chart of cumulative liquidation density."""
    price_arr = np.linspace(data.price_min, data.price_max,
                            data.price_bins)
    liq_long = data.liq_long.copy()
    liq_short = data.liq_short.copy()

    # Smooth for cleaner look
    kernel_size = max(data.price_bins // 40, 3)
    if kernel_size % 2 == 0:
        kernel_size += 1
    kernel = np.ones(kernel_size) / kernel_size
    liq_long = np.convolve(liq_long, kernel, mode="same")
    liq_short = np.convolve(liq_short, kernel, mode="same")

    bar_h = (data.price_max - data.price_min) / data.price_bins

    # Longs (green, below price) and Shorts (red, above price)
    max_val = max(liq_long.max(), liq_short.max(), 1e-6)

    ax_prof.barh(price_arr, liq_long / max_val, height=bar_h,
                 color=CANDLE_UP, alpha=0.50, zorder=2, linewidth=0)
    ax_prof.barh(price_arr, liq_short / max_val, height=bar_h,
                 color=CANDLE_DN, alpha=0.50, zorder=2, linewidth=0)

    ax_prof.set_xlim(0, 1.1)

    # Label
    ax_prof.text(0.5, 0.98, "Liq", transform=ax_prof.transAxes,
                 color=TEXT_CLR, fontsize=6, ha="center", va="top",
                 fontweight="bold")


# ══════════════════════════════════════════════════════════════════
#  CANDLESTICK DRAWING
# ══════════════════════════════════════════════════════════════════

def _draw_candlesticks(ax, timestamps, opens, highs, lows, closes,
                       t0_mdate, t1_mdate, zorder=3):
    """Draw prominent OHLC candlesticks on the heatmap.

    Always resamples to ~60-80 visible candles so each one is
    clearly readable even on a 1200px-wide chart.
    """
    from matplotlib.patches import Rectangle
    from matplotlib.collections import PatchCollection, LineCollection

    n = len(timestamps)
    if n < 2:
        return

    # Convert epoch → mdate
    mdates_arr = np.array([
        mdates.date2num(datetime.fromtimestamp(ts, tz=timezone.utc))
        for ts in timestamps
    ])

    # Always resample to target ~60-80 candles for readability
    target_candles = 72
    if n > target_candles:
        factor = max(round(n / target_candles), 2)
        mdates_arr, opens, highs, lows, closes = _resample_ohlc(
            mdates_arr, opens, highs, lows, closes, factor,
        )
        n = len(mdates_arr)

    # Body width — 70% of candle spacing
    if n > 1:
        body_w = np.median(np.diff(mdates_arr)) * 0.70
    else:
        body_w = (t1_mdate - t0_mdate) * 0.008

    # Pre-build line segments for wicks and patches for bodies
    wick_segs_up = []
    wick_segs_dn = []
    patches_up = []
    patches_dn = []

    price_range = max(highs.max() - lows.min(), 1e-8)
    min_body = price_range * 0.003  # minimum body height (0.3% of range)

    for i in range(n):
        x = float(mdates_arr[i])
        o, h, l, c = float(opens[i]), float(highs[i]), float(lows[i]), float(closes[i])

        is_up = c >= o
        body_lo = min(o, c)
        body_hi = max(o, c)
        body_height = max(body_hi - body_lo, min_body)

        rect = Rectangle(
            (x - body_w / 2, body_lo), body_w, body_height,
        )

        # Wick: line from low to high
        seg = [(x, l), (x, h)]

        if is_up:
            patches_up.append(rect)
            wick_segs_up.append(seg)
        else:
            patches_dn.append(rect)
            wick_segs_dn.append(seg)

    # Draw wicks as LineCollections (fast, single draw call)
    if wick_segs_up:
        lc_up = LineCollection(wick_segs_up, colors=CANDLE_UP,
                               linewidths=1.0, zorder=zorder)
        ax.add_collection(lc_up)
    if wick_segs_dn:
        lc_dn = LineCollection(wick_segs_dn, colors=CANDLE_DN,
                               linewidths=1.0, zorder=zorder)
        ax.add_collection(lc_dn)

    # Draw bodies as PatchCollections
    if patches_up:
        pc_up = PatchCollection(patches_up, facecolors=CANDLE_UP,
                                edgecolors=CANDLE_UP, linewidths=0.5,
                                alpha=0.80, zorder=zorder + 1)
        ax.add_collection(pc_up)
    if patches_dn:
        pc_dn = PatchCollection(patches_dn, facecolors=CANDLE_DN,
                                edgecolors=CANDLE_DN, linewidths=0.5,
                                alpha=0.80, zorder=zorder + 1)
        ax.add_collection(pc_dn)


def _resample_ohlc(mdates_arr, opens, highs, lows, closes, factor):
    """Down-sample OHLC arrays by grouping `factor` candles."""
    n = len(mdates_arr) // factor * factor
    if n == 0:
        return mdates_arr, opens, highs, lows, closes

    mdates_arr = mdates_arr[:n].reshape(-1, factor)
    opens  = opens[:n].reshape(-1, factor)
    highs  = highs[:n].reshape(-1, factor)
    lows   = lows[:n].reshape(-1, factor)
    closes = closes[:n].reshape(-1, factor)

    return (
        mdates_arr[:, factor // 2],   # middle timestamp
        opens[:, 0],                   # first open
        highs.max(axis=1),            # highest high
        lows.min(axis=1),             # lowest low
        closes[:, -1],                 # last close
    )


# ══════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════

def _gaussian_blur(arr: np.ndarray,
                   sigma_y: float = 4.0,
                   sigma_x: float = 2.5) -> np.ndarray:
    """Separable Gaussian blur via 1-D convolutions (pure NumPy)."""

    def _kernel(sigma):
        size = max(int(sigma * 6) | 1, 3)
        x = np.arange(size) - size // 2
        k = np.exp(-0.5 * (x / sigma) ** 2)
        return k / k.sum()

    ky, kx = _kernel(sigma_y), _kernel(sigma_x)

    # axis-0 (price / Y)
    py = len(ky) // 2
    padded = np.pad(arr, ((py, py), (0, 0)), mode="constant")
    buf = np.zeros_like(arr, dtype=np.float64)
    for i, w in enumerate(ky):
        buf += padded[i: i + arr.shape[0], :] * w

    # axis-1 (time / X)
    px = len(kx) // 2
    padded = np.pad(buf, ((0, 0), (px, px)), mode="constant")
    out = np.zeros_like(buf)
    for i, w in enumerate(kx):
        out += padded[:, i: i + arr.shape[1]] * w

    return out


# ── Formatters ───────────────────────────────────────────────────

def _fmt_price(price: float) -> str:
    if price >= 10_000:
        return f"{price:,.0f}"
    if price >= 1_000:
        return f"{price:,.1f}"
    if price >= 1:
        return f"{price:.2f}"
    if price >= 0.01:
        return f"{price:.4f}"
    return f"{price:.6f}"


def _fmt_usd(value: float) -> str:
    if value >= 1e9:
        return f"{value / 1e9:.2f}B"
    if value >= 1e6:
        return f"{value / 1e6:.1f}M"
    if value >= 1e3:
        return f"{value / 1e3:.0f}K"
    return f"{value:.0f}"
