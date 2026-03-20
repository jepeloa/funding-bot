"""
Binance Copy-Trading Scraper
=============================
Fetches copy-trader rankings and individual trader details from Binance.

Binance protects these endpoints behind CloudFront WAF that blocks raw HTTP
requests (403).  We use Playwright (headless Chromium) to obtain a valid
browser session and then replay the internal bapi/ endpoints with the cookies
the browser acquired.

All three public methods use the same fast path: a single long-lived page
that makes direct JS fetch() calls against Binance's internal API.

Three usage modes:
  1. search_top_traders       – paginated ranking list (POST query-list)
  2. get_trader_detail        – full detail for a single portfolioId
  3. scan_symbol_positions    – aggregate open positions across top traders for a symbol
  4. get_trader_history        – full trade history with strategy analysis
"""

import asyncio
import json
import math
import time as _time
from typing import Any

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# ── Constants ──────────────────────────────────────────────────────────────

_BASE = "https://www.binance.com"
_RANKING_URL = f"{_BASE}/es-AR/copy-trading"

_EP_QUERY_LIST = f"{_BASE}/bapi/futures/v1/friendly/future/copy-trade/home-page/query-list"
_EP_DAILY_PICKS = f"{_BASE}/bapi/futures/v1/friendly/future/copy-trade/home-page/daily-picks"
_EP_DETAIL = f"{_BASE}/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/detail"
_EP_POSITIONS = f"{_BASE}/bapi/futures/v1/friendly/future/copy-trade/lead-data/positions"
_EP_CHART = f"{_BASE}/bapi/futures/v1/public/future/copy-trade/lead-portfolio/chart-data"
_EP_PERF_COIN = f"{_BASE}/bapi/futures/v1/public/future/copy-trade/lead-portfolio/performance/coin"
_EP_POSITION_HISTORY = f"{_BASE}/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/position-history"

# Binance silently caps pageSize to 30 for query-list; position-history allows 200.
_MAX_PAGE_SIZE = 30
_HISTORY_PAGE_SIZE = 200


class CopyTradingClient:
    """Async client that scrapes Binance copy-trading data via Playwright."""

    def __init__(self):
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._playwright = None
        self._lock = asyncio.Lock()
        self._api_page: Page | None = None  # long-lived page for direct JS fetch calls

    # ── lifecycle ──────────────────────────────────────────────────────────

    async def _ensure_context(self) -> BrowserContext:
        """Launch browser + warm up a context with valid Binance cookies."""
        async with self._lock:
            if self._context is not None:
                return self._context

            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=True)
            self._context = await self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                locale="es-AR",
            )
            return self._context

    async def close(self):
        """Shutdown browser."""
        if self._api_page:
            await self._api_page.close()
            self._api_page = None
        if self._browser:
            await self._browser.close()
            self._browser = None
            self._context = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

    # ── internal: direct JS fetch from a warm page ─────────────────────────

    async def _get_api_page(self) -> Page:
        """Get (or create) a long-lived page for making JS fetch() calls.

        Single warmup: creates the browser context (if needed) and navigates
        once to _RANKING_URL to pass CloudFront anti-bot.
        """
        if self._api_page and not self._api_page.is_closed():
            return self._api_page
        ctx = await self._ensure_context()
        self._api_page = await ctx.new_page()
        try:
            await self._api_page.goto(_RANKING_URL, wait_until="domcontentloaded", timeout=30000)
            await self._api_page.wait_for_timeout(3000)
        except Exception:
            pass
        return self._api_page

    async def _js_fetch_get(self, url: str) -> dict | None:
        """Make a GET request via JS fetch() from the warm page."""
        page = await self._get_api_page()
        try:
            result = await page.evaluate(f"""
                async () => {{
                    const r = await fetch('{url}', {{ credentials: 'include' }});
                    if (!r.ok) return null;
                    return await r.json();
                }}
            """)
            return result
        except Exception:
            return None

    async def _js_fetch_post(self, url: str, body: dict) -> dict | None:
        """Make a POST request via JS fetch() from the warm page."""
        page = await self._get_api_page()
        body_json = json.dumps(body)
        try:
            result = await page.evaluate(f"""
                async () => {{
                    const r = await fetch('{url}', {{
                        method: 'POST',
                        headers: {{'Content-Type': 'application/json'}},
                        credentials: 'include',
                        body: {json.dumps(body_json)}
                    }});
                    if (!r.ok) return null;
                    return await r.json();
                }}
            """)
            return result
        except Exception:
            return None

    async def _js_batch_fetch(self, urls: list[str]) -> list[dict | None]:
        """Fetch multiple GET URLs in parallel via Promise.allSettled."""
        page = await self._get_api_page()
        urls_json = json.dumps(urls)
        try:
            results = await page.evaluate(f"""
                async () => {{
                    const urls = {urls_json};
                    const promises = urls.map(url =>
                        fetch(url, {{ credentials: 'include' }})
                        .then(r => r.ok ? r.json() : null)
                        .catch(() => null)
                    );
                    return await Promise.allSettled(promises);
                }}
            """)
            return [
                item["value"] if item.get("status") == "fulfilled" else None
                for item in results
            ]
        except Exception:
            return [None] * len(urls)

    async def _js_batch_positions(self, portfolio_ids: list[str]) -> dict[str, list]:
        """Fetch positions for multiple traders in parallel via Promise.allSettled."""
        urls = [f"{_EP_POSITIONS}?portfolioId={pid}" for pid in portfolio_ids]
        results = await self._js_batch_fetch(urls)
        out: dict[str, list] = {}
        for pid, resp in zip(portfolio_ids, results):
            if resp and resp.get("code") == "000000":
                out[pid] = resp.get("data") or []
            else:
                out[pid] = []
        return out

    # ── public API ────────────────────────────────────────────────────────

    async def search_top_traders(
        self,
        page_number: int = 1,
        page_size: int = 18,
        time_range: str = "30D",
        sort_by: str = "ROI",
        order: str = "DESC",
        nickname: str = "",
        hide_full: bool = False,
        portfolio_type: str = "ALL",
    ) -> dict[str, Any]:
        """
        Fetch the copy-trading leaderboard.

        Parameters
        ----------
        page_number : page (1-indexed)
        page_size : results per page (max 30, Binance hard cap)
        time_range : "7D" | "30D" | "90D"
        sort_by : "ROI" | "PNL" | "WIN_RATE" | "MDD" | "AUM" | "COPIER_PNL"
        order : "DESC" | "ASC"
        nickname : search by trader name (partial match)
        hide_full : hide traders that have no available slots
        portfolio_type : "ALL" | "PUBLIC"
        """
        page_size = min(page_size, _MAX_PAGE_SIZE)

        resp = await self._js_fetch_post(_EP_QUERY_LIST, {
            "pageNumber": page_number,
            "pageSize": page_size,
            "timeRange": time_range,
            "dataType": sort_by,
            "favoriteOnly": False,
            "hideFull": hide_full,
            "nickname": nickname,
            "order": order,
            "userAsset": 0,
            "portfolioType": portfolio_type,
            "useAiRecommended": False,
        })

        result: dict[str, Any] = {
            "query": {
                "page": page_number,
                "page_size": page_size,
                "time_range": time_range,
                "sort_by": sort_by,
                "order": order,
                "nickname": nickname or None,
                "hide_full": hide_full,
                "portfolio_type": portfolio_type,
            },
        }

        if resp and resp.get("code") == "000000":
            data = resp["data"]
            result["total"] = data.get("total", 0)
            result["traders"] = [_normalize_trader_summary(t) for t in data.get("list", [])]
        else:
            result["total"] = 0
            result["traders"] = []
            result["error"] = "No se pudo obtener el listado de traders"

        # Daily picks only on first page
        if page_number == 1:
            picks_resp = await self._js_fetch_get(_EP_DAILY_PICKS)
            if picks_resp and picks_resp.get("code") == "000000":
                result["daily_picks"] = [
                    _normalize_trader_summary(t)
                    for t in picks_resp["data"].get("list", [])
                ]

        return result

    async def get_trader_detail(
        self,
        portfolio_id: str,
        time_range: str = "7D",
    ) -> dict[str, Any]:
        """
        Fetch full detail for a specific copy-trader.

        Makes 4 parallel JS fetch calls for: detail, positions, chart, coin performance.

        Parameters
        ----------
        portfolio_id : the leadPortfolioId (from URL or search results)
        time_range : "7D" | "30D" | "90D" for chart/performance data
        """
        urls = [
            f"{_EP_DETAIL}?portfolioId={portfolio_id}",
            f"{_EP_POSITIONS}?portfolioId={portfolio_id}",
            f"{_EP_CHART}?dataType=ROI&portfolioId={portfolio_id}&timeRange={time_range}",
            f"{_EP_PERF_COIN}?portfolioId={portfolio_id}&timeRange={time_range}",
        ]
        responses = await self._js_batch_fetch(urls)

        detail_resp, positions_resp, chart_resp, coin_resp = responses

        result: dict[str, Any] = {"portfolio_id": portfolio_id, "time_range": time_range}

        # ── Profile + stats ──
        detail_data = None
        if detail_resp and detail_resp.get("code") == "000000":
            detail_data = detail_resp["data"]

        if detail_data:
            result["profile"] = {
                "nickname": detail_data.get("nickname"),
                "nickname_translate": detail_data.get("nicknameTranslate"),
                "status": detail_data.get("status"),
                "description": detail_data.get("descTranslate") or detail_data.get("description"),
                "avatar_url": detail_data.get("avatarUrl"),
                "badge": detail_data.get("badgeName"),
                "tags": detail_data.get("tag", []),
                "start_time": detail_data.get("startTime"),
            }
            result["stats"] = {
                "margin_balance_usdt": detail_data.get("marginBalance"),
                "aum_usdt": detail_data.get("aumAmount"),
                "copier_pnl_usdt": detail_data.get("copierPnl"),
                "profit_sharing_pct": detail_data.get("profitSharingRate"),
                "current_copiers": detail_data.get("currentCopyCount"),
                "max_copiers": detail_data.get("maxCopyCount"),
                "total_copiers_historical": detail_data.get("totalCopyCount"),
                "favorites": detail_data.get("favoriteCount"),
                "win_rate_pct": detail_data.get("winRate"),
                "mdd_pct": detail_data.get("mdd"),
                "sharp_ratio": detail_data.get("sharpRatio"),
            }
            perf = detail_data.get("performanceRetList", [])
            if perf:
                result["performance_periods"] = [
                    {"period": p.get("periodType"), "roi_pct": p.get("value")}
                    for p in perf
                ]
        else:
            result["error"] = "No se pudo obtener el detalle del trader"

        # ── ROI chart ──
        if chart_resp and chart_resp.get("code") == "000000":
            result["roi_chart"] = [
                {"date": c["dateTime"], "roi_pct": c["value"]}
                for c in chart_resp.get("data") or []
            ]

        # ── Coin distribution ──
        if coin_resp and coin_resp.get("code") == "000000":
            result["coin_distribution"] = (coin_resp.get("data") or {}).get("data", [])

        # ── Open positions ──
        if positions_resp and positions_resp.get("code") == "000000":
            positions_data = positions_resp.get("data") or []
            result["open_positions"] = [
                _normalize_position(pos) for pos in positions_data
                if float(pos.get("positionAmount", "0")) != 0
            ]
            result["total_position_symbols"] = len(set(
                pos.get("symbol") for pos in positions_data
            ))

        return result

    async def scan_symbol_positions(
        self,
        symbol: str,
        top_n: int = 100,
        sort_by: str = "AUM",
        time_range: str = "30D",
    ) -> dict[str, Any]:
        """
        Scan top N copy-traders and aggregate their open positions for a symbol.

        Returns a breakdown of how many traders hold LONG vs SHORT positions,
        estimated notional volume per side, and individual trader details.

        Parameters
        ----------
        symbol : e.g. "ETHUSDT" (will be uppercased)
        top_n : how many top traders to scan (default 100, max 200)
        sort_by : ranking criterion — "AUM" recommended for volume estimation
        time_range : "7D" | "30D" | "90D"
        """
        symbol = symbol.upper()
        top_n = min(top_n, 200)

        # Step 1: Get trader list (paginated, Binance caps at 30 per page)
        pages_needed = math.ceil(top_n / _MAX_PAGE_SIZE)
        all_traders: list[dict] = []
        total_available = 0
        resp: dict | None = None

        for pg in range(1, pages_needed + 1):
            resp = await self._js_fetch_post(_EP_QUERY_LIST, {
                "pageNumber": pg,
                "pageSize": _MAX_PAGE_SIZE,
                "timeRange": time_range,
                "dataType": sort_by,
                "favoriteOnly": False,
                "hideFull": False,
                "nickname": "",
                "order": "DESC",
                "userAsset": 0,
                "portfolioType": "ALL",
                "useAiRecommended": False,
            })
            if resp and resp.get("code") == "000000":
                page_list = resp["data"].get("list", [])
                total_available = resp["data"].get("total", 0)
                all_traders.extend(page_list)
            if len(all_traders) >= top_n:
                break

        all_traders = all_traders[:top_n]

        # Build lookup: portfolio_id -> {nickname, aum, ...}
        trader_info: dict[str, dict] = {}
        for t in all_traders:
            pid = t["leadPortfolioId"]
            trader_info[pid] = {
                "nickname": t.get("nickname", ""),
                "aum_usdt": float(t.get("aum") or 0),
                "roi_pct": float(t.get("roi") or 0),
                "win_rate_pct": float(t.get("winRate") or 0),
                "copiers": t.get("currentCopyCount", 0),
                "badge": t.get("badgeName"),
            }

        # Step 2: Batch-fetch positions (in chunks of 20 for parallelism)
        pids = list(trader_info.keys())
        chunk_size = 20
        all_positions: dict[str, list] = {}

        for i in range(0, len(pids), chunk_size):
            chunk = pids[i : i + chunk_size]
            batch = await self._js_batch_positions(chunk)
            all_positions.update(batch)

        # Step 3: Aggregate positions for the requested symbol
        longs: list[dict] = []
        shorts: list[dict] = []
        total_long_notional = 0.0
        total_short_notional = 0.0
        traders_scanned = len(pids)
        traders_with_position = 0

        for pid, positions in all_positions.items():
            info = trader_info.get(pid, {})
            for pos in positions:
                if pos.get("symbol") != symbol:
                    continue
                amt = float(pos.get("positionAmount", "0"))
                if amt == 0:
                    continue

                entry_price = float(pos.get("entryPrice", "0"))
                mark_price = float(pos.get("markPrice", "0"))
                leverage = int(pos.get("leverage", 1))
                notional = abs(amt) * mark_price
                pnl = float(pos.get("unrealizedProfit", "0"))
                side = pos.get("positionSide", "BOTH")

                # Determine effective direction
                if side == "LONG" or (side == "BOTH" and amt > 0):
                    direction = "LONG"
                elif side == "SHORT" or (side == "BOTH" and amt < 0):
                    direction = "SHORT"
                else:
                    continue

                record = {
                    "portfolio_id": pid,
                    "nickname": info.get("nickname", ""),
                    "aum_usdt": info.get("aum_usdt", 0),
                    "direction": direction,
                    "amount": abs(amt),
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "notional_usdt": round(notional, 2),
                    "unrealized_pnl_usdt": round(pnl, 4),
                    "leverage": leverage,
                    "roi_pct": info.get("roi_pct", 0),
                    "win_rate_pct": info.get("win_rate_pct", 0),
                    "copiers": info.get("copiers", 0),
                    "badge": info.get("badge"),
                }

                if direction == "LONG":
                    longs.append(record)
                    total_long_notional += notional
                else:
                    shorts.append(record)
                    total_short_notional += notional

                traders_with_position += 1

        # Sort by notional (biggest first)
        longs.sort(key=lambda x: x["notional_usdt"], reverse=True)
        shorts.sort(key=lambda x: x["notional_usdt"], reverse=True)

        total_notional = total_long_notional + total_short_notional
        long_pct = (total_long_notional / total_notional * 100) if total_notional > 0 else 0
        short_pct = (total_short_notional / total_notional * 100) if total_notional > 0 else 0

        long_aum_total = sum(r["aum_usdt"] for r in longs)
        short_aum_total = sum(r["aum_usdt"] for r in shorts)

        return {
            "symbol": symbol,
            "scan_config": {
                "top_n_traders_scanned": traders_scanned,
                "total_traders_available": total_available,
                "sort_criterion": sort_by,
                "time_range": time_range,
            },
            "summary": {
                "traders_with_position": traders_with_position,
                "long_count": len(longs),
                "short_count": len(shorts),
                "total_long_notional_usdt": round(total_long_notional, 2),
                "total_short_notional_usdt": round(total_short_notional, 2),
                "long_pct": round(long_pct, 1),
                "short_pct": round(short_pct, 1),
                "long_aum_behind_usdt": round(long_aum_total, 2),
                "short_aum_behind_usdt": round(short_aum_total, 2),
                "net_bias": "LONG" if total_long_notional > total_short_notional else (
                    "SHORT" if total_short_notional > total_long_notional else "NEUTRAL"
                ),
                "bias_ratio": round(
                    max(total_long_notional, total_short_notional) /
                    max(min(total_long_notional, total_short_notional), 1), 2
                ),
            },
            "longs": longs,
            "shorts": shorts,
        }

    async def get_trader_history(
        self,
        portfolio_id: str,
        days: int = 7,
    ) -> dict[str, Any]:
        """
        Fetch the closed-trade history for a copy-trader and produce strategy analysis.

        Parameters
        ----------
        portfolio_id : the leadPortfolioId
        days : how many days back to fetch (from now)
        """
        cutoff_ms = int((_time.time() - days * 86400) * 1000)

        # Paginate through position-history until we pass the cutoff
        all_trades: list[dict] = []
        page_num = 0
        done = False

        while not done:
            page_num += 1
            resp = await self._js_fetch_post(_EP_POSITION_HISTORY, {
                "pageNumber": page_num,
                "pageSize": _HISTORY_PAGE_SIZE,
                "portfolioId": portfolio_id,
                "sort": "OPENING",
            })
            if not resp or resp.get("code") != "000000":
                break

            page_list = resp["data"].get("list", [])
            if not page_list:
                break

            for t in page_list:
                opened = t.get("opened", 0)
                if opened >= cutoff_ms:
                    all_trades.append(t)
                else:
                    done = True
                    break

            # If we got fewer items than page size, no more pages
            if len(page_list) < _HISTORY_PAGE_SIZE:
                break

        total_in_period = len(all_trades)

        # Normalize each trade
        trades = [_normalize_trade(t) for t in all_trades]

        # ── Strategy analysis ──
        analysis = _analyze_trades(trades, days)

        return {
            "portfolio_id": portfolio_id,
            "days": days,
            "total_trades": total_in_period,
            "analysis": analysis,
            "trades": trades,
        }


# ── Normalization helpers ─────────────────────────────────────────────────

def _normalize_trader_summary(t: dict) -> dict:
    """Normalize a trader summary from the query-list or daily-picks API."""
    return {
        "portfolio_id": t.get("leadPortfolioId"),
        "nickname": t.get("nickname"),
        "roi_pct": t.get("roi"),
        "pnl_usdt": t.get("pnl"),
        "aum_usdt": t.get("aum"),
        "mdd_pct": t.get("mdd"),
        "win_rate_pct": t.get("winRate"),
        "sharp_ratio": t.get("sharpRatio"),
        "copiers": t.get("currentCopyCount"),
        "max_copiers": t.get("maxCopyCount"),
        "slots_available": (t.get("maxCopyCount") or 0) - (t.get("currentCopyCount") or 0),
        "badge": t.get("badgeName"),
        "portfolio_type": t.get("portfolioType"),
    }


def _normalize_position(pos: dict) -> dict:
    """Normalize a single position — consistent field names/types across endpoints."""
    amt = float(pos.get("positionAmount", "0"))
    side = pos.get("positionSide", "BOTH")

    if side == "LONG" or (side == "BOTH" and amt > 0):
        direction = "LONG"
    elif side == "SHORT" or (side == "BOTH" and amt < 0):
        direction = "SHORT"
    else:
        direction = side  # keep raw for zero-amount (shouldn't reach here)

    return {
        "symbol": pos.get("symbol"),
        "direction": direction,
        "amount": abs(amt),
        "entry_price": float(pos.get("entryPrice", "0")),
        "mark_price": float(pos.get("markPrice", "0")),
        "unrealized_pnl_usdt": float(pos.get("unrealizedProfit", "0")),
        "leverage": int(pos.get("leverage", 1)),
        "isolated": pos.get("isolated"),
    }


def _normalize_trade(t: dict) -> dict:
    """Normalize a single closed trade from position-history."""
    opened_ms = t.get("opened", 0)
    closed_ms = t.get("closed", 0)
    duration_s = (closed_ms - opened_ms) / 1000 if closed_ms and opened_ms else 0

    avg_cost = float(t.get("avgCost", 0))
    avg_close = float(t.get("avgClosePrice", 0))
    pnl = float(t.get("closingPnl", 0))
    volume = float(t.get("closedVolume", 0))
    max_size = float(t.get("maxOpenInterest", 0))
    notional = volume * avg_cost if avg_cost else 0

    side_raw = (t.get("side") or "").capitalize()
    direction = "LONG" if side_raw == "Long" else ("SHORT" if side_raw == "Short" else side_raw)

    return {
        "id": t.get("id"),
        "symbol": t.get("symbol"),
        "direction": direction,
        "opened_ts": opened_ms,
        "closed_ts": closed_ms,
        "duration_seconds": round(duration_s),
        "duration_human": _duration_human(duration_s),
        "avg_entry_price": avg_cost,
        "avg_close_price": avg_close,
        "pnl_usdt": round(pnl, 4),
        "max_size": max_size,
        "closed_volume": volume,
        "notional_usdt": round(notional, 2),
        "margin_mode": t.get("isolated", "Cross"),
        "status": t.get("status"),
    }


def _duration_human(seconds: float) -> str:
    """Convert seconds to human-readable duration."""
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    hours = int(seconds // 3600)
    mins = int((seconds % 3600) // 60)
    if hours < 24:
        return f"{hours}h {mins}m"
    days = hours // 24
    hours = hours % 24
    return f"{days}d {hours}h"


def _analyze_trades(trades: list[dict], days: int) -> dict[str, Any]:
    """Produce strategy analysis from a list of normalized trades."""
    if not trades:
        return {"error": "Sin trades en el período"}

    wins = [t for t in trades if t["pnl_usdt"] > 0]
    losses = [t for t in trades if t["pnl_usdt"] < 0]
    breakeven = [t for t in trades if t["pnl_usdt"] == 0]

    total_pnl = sum(t["pnl_usdt"] for t in trades)
    total_win_pnl = sum(t["pnl_usdt"] for t in wins)
    total_loss_pnl = sum(t["pnl_usdt"] for t in losses)

    win_rate = len(wins) / len(trades) * 100 if trades else 0
    avg_win = total_win_pnl / len(wins) if wins else 0
    avg_loss = total_loss_pnl / len(losses) if losses else 0
    profit_factor = abs(total_win_pnl / total_loss_pnl) if total_loss_pnl else float("inf")
    expectancy = total_pnl / len(trades) if trades else 0

    # Risk/reward ratio: avg win / abs(avg loss)
    rr_ratio = abs(avg_win / avg_loss) if avg_loss else float("inf")

    # Duration analysis
    durations = [t["duration_seconds"] for t in trades if t["duration_seconds"] > 0]
    avg_duration = sum(durations) / len(durations) if durations else 0
    min_duration = min(durations) if durations else 0
    max_duration = max(durations) if durations else 0
    median_duration = sorted(durations)[len(durations) // 2] if durations else 0

    # Symbol breakdown
    symbol_stats: dict[str, dict] = {}
    for t in trades:
        sym = t["symbol"]
        if sym not in symbol_stats:
            symbol_stats[sym] = {"trades": 0, "wins": 0, "pnl_usdt": 0.0, "volume_usdt": 0.0}
        symbol_stats[sym]["trades"] += 1
        if t["pnl_usdt"] > 0:
            symbol_stats[sym]["wins"] += 1
        symbol_stats[sym]["pnl_usdt"] += t["pnl_usdt"]
        symbol_stats[sym]["volume_usdt"] += t["notional_usdt"]

    # Finalize per-symbol stats
    symbols_breakdown = []
    for sym, ss in sorted(symbol_stats.items(), key=lambda x: x[1]["pnl_usdt"], reverse=True):
        symbols_breakdown.append({
            "symbol": sym,
            "trades": ss["trades"],
            "wins": ss["wins"],
            "win_rate_pct": round(ss["wins"] / ss["trades"] * 100, 1) if ss["trades"] else 0,
            "pnl_usdt": round(ss["pnl_usdt"], 4),
            "volume_usdt": round(ss["volume_usdt"], 2),
        })

    # Direction breakdown
    longs = [t for t in trades if t["direction"] == "LONG"]
    shorts = [t for t in trades if t["direction"] == "SHORT"]
    long_pnl = sum(t["pnl_usdt"] for t in longs)
    short_pnl = sum(t["pnl_usdt"] for t in shorts)
    long_wins = sum(1 for t in longs if t["pnl_usdt"] > 0)
    short_wins = sum(1 for t in shorts if t["pnl_usdt"] > 0)

    # Consecutive streaks
    max_win_streak = 0
    max_loss_streak = 0
    current_streak = 0
    last_was_win = None
    for t in trades:
        is_win = t["pnl_usdt"] > 0
        if is_win == last_was_win:
            current_streak += 1
        else:
            current_streak = 1
            last_was_win = is_win
        if is_win:
            max_win_streak = max(max_win_streak, current_streak)
        else:
            max_loss_streak = max(max_loss_streak, current_streak)

    # Best and worst trades
    best = max(trades, key=lambda t: t["pnl_usdt"])
    worst = min(trades, key=lambda t: t["pnl_usdt"])

    # Trades per day
    trades_per_day = len(trades) / days if days > 0 else len(trades)

    return {
        "period_days": days,
        "total_trades": len(trades),
        "trades_per_day": round(trades_per_day, 1),
        "total_pnl_usdt": round(total_pnl, 4),
        "win_rate_pct": round(win_rate, 1),
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(breakeven),
        "avg_win_usdt": round(avg_win, 4),
        "avg_loss_usdt": round(avg_loss, 4),
        "biggest_win": {"symbol": best["symbol"], "pnl_usdt": best["pnl_usdt"], "direction": best["direction"]},
        "biggest_loss": {"symbol": worst["symbol"], "pnl_usdt": worst["pnl_usdt"], "direction": worst["direction"]},
        "profit_factor": round(profit_factor, 2) if profit_factor != float("inf") else "inf",
        "expectancy_usdt": round(expectancy, 4),
        "risk_reward_ratio": round(rr_ratio, 2) if rr_ratio != float("inf") else "inf",
        "max_win_streak": max_win_streak,
        "max_loss_streak": max_loss_streak,
        "duration": {
            "avg": _duration_human(avg_duration),
            "median": _duration_human(median_duration),
            "min": _duration_human(min_duration),
            "max": _duration_human(max_duration),
            "avg_seconds": round(avg_duration),
        },
        "direction_breakdown": {
            "long_trades": len(longs),
            "long_wins": long_wins,
            "long_win_rate_pct": round(long_wins / len(longs) * 100, 1) if longs else 0,
            "long_pnl_usdt": round(long_pnl, 4),
            "short_trades": len(shorts),
            "short_wins": short_wins,
            "short_win_rate_pct": round(short_wins / len(shorts) * 100, 1) if shorts else 0,
            "short_pnl_usdt": round(short_pnl, 4),
        },
        "top_symbols": symbols_breakdown[:15],
    }
