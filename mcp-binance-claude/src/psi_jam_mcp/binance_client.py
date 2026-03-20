"""
Binance Futures API client for L1 and L2 data.
Uses public endpoints only — no API key required.
"""

import asyncio
import httpx
import time
from typing import Optional

BASE_URL = "https://fapi.binance.com"
DATA_URL = "https://fapi.binance.com"

VALID_INTERVALS = [
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
]


class BinanceClient:
    """Async client for Binance USDT-M Futures public API."""

    def __init__(self, timeout: float = 30.0):
        self.client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=timeout,
            headers={"User-Agent": "PsiJamMCP/0.1"},
        )

    async def close(self):
        await self.client.aclose()

    # ─────────────────────────────────────────────
    # L1 DATA
    # ─────────────────────────────────────────────

    async def get_klines(
        self,
        symbol: str,
        interval: str = "1h",
        limit: int = 500,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> list[dict]:
        """
        Fetch OHLCV klines (candlestick data).
        Returns list of dicts with: open_time, open, high, low, close, volume,
        close_time, quote_volume, trades, taker_buy_volume, taker_buy_quote_volume.
        """
        params = {"symbol": symbol.upper(), "interval": interval, "limit": min(limit, 1500)}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time

        resp = await self.client.get("/fapi/v1/klines", params=params)
        resp.raise_for_status()
        raw = resp.json()

        return [
            {
                "open_time": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": int(k[6]),
                "quote_volume": float(k[7]),
                "trades": int(k[8]),
                "taker_buy_volume": float(k[9]),
                "taker_buy_quote_volume": float(k[10]),
            }
            for k in raw
        ]

    async def get_recent_trades(self, symbol: str, limit: int = 500) -> list[dict]:
        """Fetch most recent trades (max 1000)."""
        resp = await self.client.get(
            "/fapi/v1/trades",
            params={"symbol": symbol.upper(), "limit": min(limit, 1000)},
        )
        resp.raise_for_status()
        return [
            {
                "id": t["id"],
                "price": float(t["price"]),
                "qty": float(t["qty"]),
                "quote_qty": float(t["quoteQty"]),
                "time": int(t["time"]),
                "is_buyer_maker": t["isBuyerMaker"],
            }
            for t in resp.json()
        ]

    async def get_agg_trades(
        self,
        symbol: str,
        limit: int = 500,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> list[dict]:
        """Fetch aggregated trades."""
        params = {"symbol": symbol.upper(), "limit": min(limit, 1000)}
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time

        resp = await self.client.get("/fapi/v1/aggTrades", params=params)
        resp.raise_for_status()
        return [
            {
                "agg_id": t["a"],
                "price": float(t["p"]),
                "qty": float(t["q"]),
                "first_trade_id": t["f"],
                "last_trade_id": t["l"],
                "time": int(t["T"]),
                "is_buyer_maker": t["m"],
            }
            for t in resp.json()
        ]

    async def get_agg_trades_paginated(
        self,
        symbol: str,
        total: int = 5000,
    ) -> list[dict]:
        """Fetch up to `total` recent aggTrades by paging backwards from the latest."""
        all_trades: list[dict] = []
        end_time = None
        while len(all_trades) < total:
            batch_size = min(1000, total - len(all_trades))
            params: dict = {"symbol": symbol.upper(), "limit": batch_size}
            if end_time is not None:
                params["endTime"] = end_time
            resp = await self.client.get("/fapi/v1/aggTrades", params=params)
            resp.raise_for_status()
            raw = resp.json()
            if not raw:
                break
            batch = [
                {
                    "price": float(t["p"]),
                    "qty": float(t["q"]),
                    "time": int(t["T"]),
                    "is_buyer_maker": t["m"],
                }
                for t in raw
            ]
            all_trades = batch + all_trades  # prepend (older first)
            # Page backward: next endTime = earliest timestamp in this batch - 1
            end_time = int(raw[0]["T"]) - 1
            if len(raw) < batch_size:
                break  # no more data
        return all_trades

    async def get_ticker_24h(self, symbol: str) -> dict:
        """Fetch 24h ticker stats."""
        resp = await self.client.get(
            "/fapi/v1/ticker/24hr", params={"symbol": symbol.upper()}
        )
        resp.raise_for_status()
        d = resp.json()
        return {
            "symbol": d["symbol"],
            "price_change": float(d["priceChange"]),
            "price_change_pct": float(d["priceChangePercent"]),
            "weighted_avg_price": float(d["weightedAvgPrice"]),
            "last_price": float(d["lastPrice"]),
            "volume": float(d["volume"]),
            "quote_volume": float(d["quoteVolume"]),
            "open": float(d["openPrice"]),
            "high": float(d["highPrice"]),
            "low": float(d["lowPrice"]),
            "trades": int(d["count"]),
        }

    async def get_funding_rate(
        self, symbol: str, limit: int = 100, start_time: Optional[int] = None
    ) -> list[dict]:
        """Fetch funding rate history."""
        params = {"symbol": symbol.upper(), "limit": min(limit, 1000)}
        if start_time:
            params["startTime"] = start_time

        resp = await self.client.get("/fapi/v1/fundingRate", params=params)
        resp.raise_for_status()
        return [
            {
                "symbol": f["symbol"],
                "funding_rate": float(f["fundingRate"]),
                "funding_time": int(f["fundingTime"]),
                "mark_price": float(f.get("markPrice", 0)),
            }
            for f in resp.json()
        ]

    async def get_premium_index(self, symbol: str = None) -> list[dict] | dict:
        """Fetch premium index info including mark price, index price,
        estimated settle price, funding rate, and next funding time.
        If symbol is provided, returns a single dict. Otherwise returns list for all symbols."""
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        resp = await self.client.get("/fapi/v1/premiumIndex", params=params)
        resp.raise_for_status()
        raw = resp.json()

        def _parse(d: dict) -> dict:
            return {
                "symbol": d["symbol"],
                "mark_price": float(d.get("markPrice", 0)),
                "index_price": float(d.get("indexPrice", 0)),
                "estimated_settle_price": float(d.get("estimatedSettlePrice", 0)),
                "last_funding_rate": float(d.get("lastFundingRate", 0)),
                "next_funding_time": int(d.get("nextFundingTime", 0)),
                "interest_rate": float(d.get("interestRate", 0)),
                "time": int(d.get("time", 0)),
            }

        if symbol:
            if isinstance(raw, list):
                return _parse(raw[0]) if raw else {}
            return _parse(raw)
        return [_parse(d) for d in raw]

    async def get_open_interest(self, symbol: str) -> dict:
        """Fetch current open interest."""
        resp = await self.client.get(
            "/fapi/v1/openInterest", params={"symbol": symbol.upper()}
        )
        resp.raise_for_status()
        d = resp.json()
        return {
            "symbol": d["symbol"],
            "open_interest": float(d["openInterest"]),
            "time": int(d.get("time", int(time.time() * 1000))),
        }

    async def get_open_interest_hist(
        self, symbol: str, period: str = "1h", limit: int = 30
    ) -> list[dict]:
        """Fetch open interest history. Period: 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d."""
        resp = await self.client.get(
            "/futures/data/openInterestHist",
            params={"symbol": symbol.upper(), "period": period, "limit": min(limit, 500)},
        )
        resp.raise_for_status()
        return [
            {
                "symbol": d["symbol"],
                "sum_open_interest": float(d["sumOpenInterest"]),
                "sum_open_interest_value": float(d["sumOpenInterestValue"]),
                "timestamp": int(d["timestamp"]),
            }
            for d in resp.json()
        ]

    async def get_long_short_ratio(
        self, symbol: str, period: str = "1h", limit: int = 30
    ) -> list[dict]:
        """Global long/short account ratio."""
        resp = await self.client.get(
            "/futures/data/globalLongShortAccountRatio",
            params={"symbol": symbol.upper(), "period": period, "limit": min(limit, 500)},
        )
        resp.raise_for_status()
        return [
            {
                "symbol": d["symbol"],
                "long_account": float(d["longAccount"]),
                "short_account": float(d["shortAccount"]),
                "long_short_ratio": float(d["longShortRatio"]),
                "timestamp": int(d["timestamp"]),
            }
            for d in resp.json()
        ]

    async def get_taker_buy_sell_ratio(
        self, symbol: str, period: str = "1h", limit: int = 30
    ) -> list[dict]:
        """Taker buy/sell volume ratio."""
        resp = await self.client.get(
            "/futures/data/takerlongshortRatio",
            params={"symbol": symbol.upper(), "period": period, "limit": min(limit, 500)},
        )
        resp.raise_for_status()
        return [
            {
                "buy_sell_ratio": float(d["buySellRatio"]),
                "buy_vol": float(d["buyVol"]),
                "sell_vol": float(d["sellVol"]),
                "timestamp": int(d["timestamp"]),
            }
            for d in resp.json()
        ]

    # ─────────────────────────────────────────────
    # L2 DATA
    # ─────────────────────────────────────────────

    async def get_orderbook(self, symbol: str, limit: int = 100) -> dict:
        """
        Fetch L2 order book depth.
        limit: 5, 10, 20, 50, 100, 500, 1000
        Returns dict with bids, asks (each list of [price, qty]),
        plus derived metrics.
        """
        valid_limits = [5, 10, 20, 50, 100, 500, 1000]
        actual_limit = min(valid_limits, key=lambda x: abs(x - limit))

        resp = await self.client.get(
            "/fapi/v1/depth",
            params={"symbol": symbol.upper(), "limit": actual_limit},
        )
        resp.raise_for_status()
        data = resp.json()

        bids = [[float(p), float(q)] for p, q in data["bids"]]
        asks = [[float(p), float(q)] for p, q in data["asks"]]

        # Derived L2 metrics
        bid_depth = sum(q for _, q in bids)
        ask_depth = sum(q for _, q in asks)
        mid_price = (bids[0][0] + asks[0][0]) / 2 if bids and asks else 0
        spread = asks[0][0] - bids[0][0] if bids and asks else 0
        spread_bps = (spread / mid_price * 10000) if mid_price > 0 else 0

        # Imbalance at top N levels
        top_n = min(10, len(bids), len(asks))
        top_bid_depth = sum(bids[i][1] for i in range(top_n))
        top_ask_depth = sum(asks[i][1] for i in range(top_n))
        imbalance = (
            (top_bid_depth - top_ask_depth) / (top_bid_depth + top_ask_depth)
            if (top_bid_depth + top_ask_depth) > 0
            else 0
        )

        # Wall detection (orders > 3x median size)
        all_sizes = [q for _, q in bids + asks]
        if all_sizes:
            median_size = sorted(all_sizes)[len(all_sizes) // 2]
            bid_walls = [
                {"price": p, "qty": q, "distance_pct": (mid_price - p) / mid_price * 100}
                for p, q in bids
                if q > 3 * median_size
            ]
            ask_walls = [
                {"price": p, "qty": q, "distance_pct": (p - mid_price) / mid_price * 100}
                for p, q in asks
                if q > 3 * median_size
            ]
        else:
            bid_walls = []
            ask_walls = []

        return {
            "symbol": symbol.upper(),
            "last_update_id": data.get("lastUpdateId"),
            "timestamp": int(time.time() * 1000),
            "bids": bids[:50],  # Cap output to top 50 levels
            "asks": asks[:50],
            "metrics": {
                "mid_price": mid_price,
                "spread": spread,
                "spread_bps": round(spread_bps, 2),
                "bid_depth_total": bid_depth,
                "ask_depth_total": ask_depth,
                "depth_ratio": round(bid_depth / ask_depth, 4) if ask_depth > 0 else 0,
                "top10_imbalance": round(imbalance, 4),
                "bid_walls": bid_walls[:5],
                "ask_walls": ask_walls[:5],
            },
        }

    async def get_orderbook_snapshot(self, symbol: str, depth: int = 20) -> dict:
        """
        Lightweight order book snapshot with summary stats only.
        Useful for rapid polling without huge data transfer.
        """
        book = await self.get_orderbook(symbol, limit=depth)
        return {
            "symbol": book["symbol"],
            "timestamp": book["timestamp"],
            "metrics": book["metrics"],
            "top5_bids": book["bids"][:5],
            "top5_asks": book["asks"][:5],
        }

    # ─────────────────────────────────────────────
    # GLOBAL MARKET ANALYSIS
    # ─────────────────────────────────────────────

    _KEY_PAIRS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

    async def get_global_market_analysis(self) -> dict:
        """
        Aggregate global market snapshot oriented to short-trading analysis.
        Single call that returns: BTC status, market breadth, top funding,
        long/short sentiment, taker flow, top pumps and dumps.
        """
        t_start = time.time()

        # ── Parallel batch 1: tickers + exchange info ──
        ticker_resp, info_resp = await asyncio.gather(
            self.client.get("/fapi/v1/ticker/24hr"),
            self.client.get("/fapi/v1/exchangeInfo"),
        )
        ticker_resp.raise_for_status()
        info_resp.raise_for_status()

        all_tickers = ticker_resp.json()
        perpetual_symbols = {
            s["symbol"]
            for s in info_resp.json().get("symbols", [])
            if s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
        }

        # ── Parse tickers ──
        parsed = []
        btc_ticker = None
        for t in all_tickers:
            sym = t.get("symbol", "")
            if sym not in perpetual_symbols:
                continue
            try:
                change = float(t.get("priceChangePercent", 0))
                price = float(t.get("lastPrice", 0))
                qvol = float(t.get("quoteVolume", 0))
                high = float(t.get("highPrice", 0))
                low = float(t.get("lowPrice", 0))
                vol = float(t.get("volume", 0))
            except (ValueError, TypeError):
                continue
            entry = {
                "symbol": sym,
                "price": price,
                "change_pct": change,
                "quote_volume": round(qvol, 2),
                "high": high,
                "low": low,
                "volume": vol,
            }
            parsed.append(entry)
            if sym == "BTCUSDT":
                btc_ticker = entry

        # ── Market breadth ──
        changes = [p["change_pct"] for p in parsed]
        up = sum(1 for c in changes if c > 0)
        down = sum(1 for c in changes if c < 0)
        flat = len(changes) - up - down
        avg_change = sum(changes) / len(changes) if changes else 0
        median_change = sorted(changes)[len(changes) // 2] if changes else 0

        # Top pumps & dumps (by % change, min volume filter)
        vol_threshold = 5_000_000
        liquid = [p for p in parsed if p["quote_volume"] >= vol_threshold]
        liquid_sorted = sorted(liquid, key=lambda x: x["change_pct"], reverse=True)
        top_pumps = liquid_sorted[:10]
        top_dumps = liquid_sorted[-10:][::-1]  # worst 10, most negative first

        # ── Parallel batch 2: funding + L/S + taker for key pairs ──
        async def _safe_fetch(coro):
            try:
                return await coro
            except Exception:
                return None

        funding_tasks = [_safe_fetch(self.get_funding_rate(s, limit=3)) for s in self._KEY_PAIRS]
        ls_tasks = [_safe_fetch(self.get_long_short_ratio(s, period="1h", limit=1)) for s in self._KEY_PAIRS]
        taker_tasks = [_safe_fetch(self.get_taker_buy_sell_ratio(s, period="1h", limit=1)) for s in self._KEY_PAIRS]
        oi_tasks = [_safe_fetch(self.get_open_interest(s)) for s in self._KEY_PAIRS]

        all_results = await asyncio.gather(
            *funding_tasks, *ls_tasks, *taker_tasks, *oi_tasks
        )

        n = len(self._KEY_PAIRS)
        funding_results = all_results[:n]
        ls_results = all_results[n:2*n]
        taker_results = all_results[2*n:3*n]
        oi_results = all_results[3*n:4*n]

        # ── Process funding ──
        funding_summary = []
        for sym, fr in zip(self._KEY_PAIRS, funding_results):
            if fr and len(fr) > 0:
                last_rate = fr[-1]["funding_rate"]
                funding_summary.append({
                    "symbol": sym,
                    "funding_rate": last_rate,
                    "funding_pct": round(last_rate * 100, 4),
                    "shorts_paid": last_rate < 0,
                })
        # Also scan all liquid pairs for extreme funding
        extreme_funding_tasks = []
        extreme_pairs = [p["symbol"] for p in liquid_sorted[:30] if p["symbol"] not in self._KEY_PAIRS]
        extreme_funding_results = await asyncio.gather(
            *[_safe_fetch(self.get_funding_rate(s, limit=1)) for s in extreme_pairs[:15]]
        )
        for sym, fr in zip(extreme_pairs[:15], extreme_funding_results):
            if fr and len(fr) > 0:
                rate = fr[-1]["funding_rate"]
                if abs(rate) >= 0.0003:  # Only include extreme funding
                    funding_summary.append({
                        "symbol": sym,
                        "funding_rate": rate,
                        "funding_pct": round(rate * 100, 4),
                        "shorts_paid": rate < 0,
                    })
        funding_summary.sort(key=lambda x: x["funding_rate"], reverse=True)

        # ── Process L/S ratio ──
        ls_summary = []
        for sym, ls in zip(self._KEY_PAIRS, ls_results):
            if ls and len(ls) > 0:
                ls_summary.append({
                    "symbol": sym,
                    "long_pct": round(ls[-1]["long_account"] * 100, 1),
                    "short_pct": round(ls[-1]["short_account"] * 100, 1),
                    "ratio": round(ls[-1]["long_short_ratio"], 3),
                })

        # ── Process taker volume ──
        taker_summary = []
        for sym, tv in zip(self._KEY_PAIRS, taker_results):
            if tv and len(tv) > 0:
                bsr = tv[-1]["buy_sell_ratio"]
                taker_summary.append({
                    "symbol": sym,
                    "buy_sell_ratio": round(bsr, 4),
                    "sellers_dominate": bsr < 1.0,
                })

        # ── Process OI ──
        oi_summary = []
        for sym, oi in zip(self._KEY_PAIRS, oi_results):
            if oi:
                oi_summary.append({
                    "symbol": sym,
                    "open_interest": oi["open_interest"],
                })

        elapsed = round(time.time() - t_start, 2)

        return {
            "timestamp": int(time.time() * 1000),
            "elapsed_sec": elapsed,
            "btc": {
                "price": btc_ticker["price"] if btc_ticker else 0,
                "change_pct_24h": btc_ticker["change_pct"] if btc_ticker else 0,
                "high_24h": btc_ticker["high"] if btc_ticker else 0,
                "low_24h": btc_ticker["low"] if btc_ticker else 0,
            },
            "market_breadth": {
                "total_pairs": len(parsed),
                "up": up,
                "down": down,
                "flat": flat,
                "up_pct": round(up / len(parsed) * 100, 1) if parsed else 0,
                "down_pct": round(down / len(parsed) * 100, 1) if parsed else 0,
                "avg_change_pct": round(avg_change, 2),
                "median_change_pct": round(median_change, 2),
            },
            "funding": funding_summary,
            "long_short": ls_summary,
            "taker_flow": taker_summary,
            "open_interest": oi_summary,
            "top_pumps": [{
                "symbol": p["symbol"],
                "change_pct": p["change_pct"],
                "quote_volume": p["quote_volume"],
            } for p in top_pumps],
            "top_dumps": [{
                "symbol": p["symbol"],
                "change_pct": p["change_pct"],
                "quote_volume": p["quote_volume"],
            } for p in top_dumps],
        }

    # ─────────────────────────────────────────────
    # EXCHANGE INFO
    # ─────────────────────────────────────────────

    async def get_exchange_info(self, symbol: Optional[str] = None) -> dict:
        """Fetch exchange info (symbols, filters, etc.)."""
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()

        resp = await self.client.get("/fapi/v1/exchangeInfo", params=params)
        resp.raise_for_status()
        data = resp.json()

        if symbol:
            symbols = [
                s for s in data.get("symbols", []) if s["symbol"] == symbol.upper()
            ]
        else:
            # Return only USDT perpetuals
            symbols = [
                s
                for s in data.get("symbols", [])
                if s.get("contractType") == "PERPETUAL"
                and s.get("quoteAsset") == "USDT"
                and s.get("status") == "TRADING"
            ]

        return {
            "total_symbols": len(symbols),
            "symbols": [
                {
                    "symbol": s["symbol"],
                    "base_asset": s["baseAsset"],
                    "status": s["status"],
                    "contract_type": s.get("contractType", ""),
                    "price_precision": s.get("pricePrecision", 0),
                    "quantity_precision": s.get("quantityPrecision", 0),
                }
                for s in symbols[:100]  # Cap at 100
            ],
        }

    # ─────────────────────────────────────────────
    # MULTI-TIMEFRAME FETCH
    # ─────────────────────────────────────────────

    async def get_multi_tf_klines(
        self,
        symbol: str,
        intervals: list[str],
        limit: int = 200,
    ) -> dict[str, list[dict]]:
        """Fetch klines across multiple timeframes simultaneously."""
        result = {}
        for interval in intervals:
            if interval in VALID_INTERVALS:
                result[interval] = await self.get_klines(symbol, interval, limit)
        return result
