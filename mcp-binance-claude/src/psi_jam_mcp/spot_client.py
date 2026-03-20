"""
Binance Spot API Client
=======================
Async client for Binance Spot market data and authenticated trading.
Mirrors the pattern from binance_client.py (futures) but targets api.binance.com.

Public endpoints:
  - Spot prices (single + batch)
  - Spot order book
  - Spot ticker 24h
  - Exchange info (lot size, precision)

Authenticated endpoints (HMAC-SHA256):
  - Place order (BUY/SELL, MARKET/LIMIT)
  - Account balance
  - My trades
  - Cancel order

Used by the basis trading system to execute the spot leg of hedged positions.
"""

import asyncio
import hashlib
import hmac
import os
import time
import copy
import urllib.parse
from typing import Optional

import httpx

SPOT_BASE_URL = "https://api.binance.com"


class SpotClient:
    """
    Async Binance Spot API client.
    Supports both public (no auth) and authenticated (HMAC-SHA256) endpoints.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""

        headers = {"User-Agent": "PsiJamMCP/0.1"}
        if self.api_key:
            headers["X-MBX-APIKEY"] = self.api_key

        self.client = httpx.AsyncClient(
            base_url=SPOT_BASE_URL,
            timeout=timeout,
            headers=headers,
        )

        # Cache for exchange info
        self._symbol_info_cache: dict = {}

    async def close(self):
        await self.client.aclose()

    # ─────────────────────────────────────────────
    # SIGNING (same HMAC-SHA256 as futures)
    # ─────────────────────────────────────────────

    def _sign(self, params: dict) -> dict:
        """Add timestamp and HMAC-SHA256 signature to request params."""
        params["timestamp"] = int(time.time() * 1000)
        params.setdefault("recvWindow", 5000)
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        params["signature"] = signature
        return params

    async def _signed_get(self, path: str, params: dict = None) -> dict:
        """Signed GET request."""
        params = self._sign(params or {})
        resp = await self.client.get(path, params=params)
        resp.raise_for_status()
        return resp.json()

    async def _signed_post(self, path: str, params: dict = None, retries: int = 0) -> dict:
        """Signed POST request with optional retry."""
        original_params = copy.deepcopy(params or {})
        signed = self._sign(params or {})
        resp = await self.client.post(path, params=signed)

        if retries > 0 and resp.status_code >= 400:
            for attempt in range(retries):
                await asyncio.sleep(0.1 * (attempt + 1))
                retry_params = copy.deepcopy(original_params)
                signed = self._sign(retry_params)
                resp = await self.client.post(path, params=signed)
                if resp.status_code == 200:
                    return resp.json()

        resp.raise_for_status()
        return resp.json()

    async def _signed_delete(self, path: str, params: dict = None) -> dict:
        """Signed DELETE request."""
        params = self._sign(params or {})
        resp = await self.client.delete(path, params=params)
        resp.raise_for_status()
        return resp.json()

    # ─────────────────────────────────────────────
    # PUBLIC: PRICES
    # ─────────────────────────────────────────────

    async def get_price(self, symbol: str) -> dict:
        """Get current spot price for a single symbol."""
        resp = await self.client.get(
            "/api/v3/ticker/price", params={"symbol": symbol.upper()}
        )
        resp.raise_for_status()
        d = resp.json()
        return {
            "symbol": d["symbol"],
            "price": float(d["price"]),
        }

    async def get_all_prices(self) -> list[dict]:
        """Get spot prices for ALL symbols (single API call)."""
        resp = await self.client.get("/api/v3/ticker/price")
        resp.raise_for_status()
        return [
            {"symbol": d["symbol"], "price": float(d["price"])}
            for d in resp.json()
        ]

    async def get_book_ticker(self, symbol: str) -> dict:
        """Get best bid/ask (top of book) for a symbol."""
        resp = await self.client.get(
            "/api/v3/ticker/bookTicker", params={"symbol": symbol.upper()}
        )
        resp.raise_for_status()
        d = resp.json()
        return {
            "symbol": d["symbol"],
            "bid_price": float(d["bidPrice"]),
            "bid_qty": float(d["bidQty"]),
            "ask_price": float(d["askPrice"]),
            "ask_qty": float(d["askQty"]),
        }

    async def get_all_book_tickers(self) -> list[dict]:
        """Get best bid/ask for ALL symbols (single API call)."""
        resp = await self.client.get("/api/v3/ticker/bookTicker")
        resp.raise_for_status()
        return [
            {
                "symbol": d["symbol"],
                "bid_price": float(d["bidPrice"]),
                "bid_qty": float(d["bidQty"]),
                "ask_price": float(d["askPrice"]),
                "ask_qty": float(d["askQty"]),
            }
            for d in resp.json()
        ]

    async def get_ticker_24h(self, symbol: str) -> dict:
        """Fetch 24h ticker stats for spot."""
        resp = await self.client.get(
            "/api/v3/ticker/24hr", params={"symbol": symbol.upper()}
        )
        resp.raise_for_status()
        d = resp.json()
        return {
            "symbol": d["symbol"],
            "price_change": float(d["priceChange"]),
            "price_change_pct": float(d["priceChangePercent"]),
            "last_price": float(d["lastPrice"]),
            "volume": float(d["volume"]),
            "quote_volume": float(d["quoteVolume"]),
            "open": float(d["openPrice"]),
            "high": float(d["highPrice"]),
            "low": float(d["lowPrice"]),
            "trades": int(d["count"]),
        }

    # ─────────────────────────────────────────────
    # PUBLIC: ORDER BOOK
    # ─────────────────────────────────────────────

    async def get_orderbook(self, symbol: str, limit: int = 20) -> dict:
        """Fetch spot order book depth."""
        valid_limits = [5, 10, 20, 50, 100, 500, 1000, 5000]
        actual_limit = min(valid_limits, key=lambda x: abs(x - limit))

        resp = await self.client.get(
            "/api/v3/depth",
            params={"symbol": symbol.upper(), "limit": actual_limit},
        )
        resp.raise_for_status()
        data = resp.json()

        bids = [[float(p), float(q)] for p, q in data["bids"]]
        asks = [[float(p), float(q)] for p, q in data["asks"]]

        mid_price = (bids[0][0] + asks[0][0]) / 2 if bids and asks else 0
        spread = asks[0][0] - bids[0][0] if bids and asks else 0
        spread_bps = (spread / mid_price * 10000) if mid_price > 0 else 0

        return {
            "symbol": symbol.upper(),
            "mid_price": mid_price,
            "spread": spread,
            "spread_bps": round(spread_bps, 2),
            "best_bid": bids[0][0] if bids else 0,
            "best_ask": asks[0][0] if asks else 0,
            "bids": bids[:10],
            "asks": asks[:10],
        }

    # ─────────────────────────────────────────────
    # PUBLIC: EXCHANGE INFO (precision, lot size)
    # ─────────────────────────────────────────────

    async def get_symbol_info(self, symbol: str) -> dict:
        """Get symbol precision, lot size filters. Cached after first call."""
        symbol = symbol.upper()
        if symbol in self._symbol_info_cache:
            return self._symbol_info_cache[symbol]

        resp = await self.client.get("/api/v3/exchangeInfo")
        resp.raise_for_status()

        for s in resp.json().get("symbols", []):
            sym = s["symbol"]
            filters = {f["filterType"]: f for f in s.get("filters", [])}

            # Extract step sizes
            lot_size = filters.get("LOT_SIZE", {})
            price_filter = filters.get("PRICE_FILTER", {})
            notional = filters.get("NOTIONAL", {}) or filters.get("MIN_NOTIONAL", {})

            info = {
                "symbol": sym,
                "base_asset": s.get("baseAsset", ""),
                "quote_asset": s.get("quoteAsset", ""),
                "status": s.get("status", ""),
                "base_precision": s.get("baseAssetPrecision", 8),
                "quote_precision": s.get("quoteAssetPrecision", 8),
                "min_qty": float(lot_size.get("minQty", 0)),
                "max_qty": float(lot_size.get("maxQty", 0)),
                "step_size": float(lot_size.get("stepSize", 0)),
                "min_price": float(price_filter.get("minPrice", 0)),
                "tick_size": float(price_filter.get("tickSize", 0)),
                "min_notional": float(notional.get("minNotional", 0)),
            }
            self._symbol_info_cache[sym] = info

        if symbol not in self._symbol_info_cache:
            raise ValueError(f"Symbol {symbol} not found on Binance Spot")

        return self._symbol_info_cache[symbol]

    def _round_qty(self, qty: float, info: dict) -> str:
        """Round quantity to symbol's step size."""
        step = info.get("step_size", 0)
        if step > 0:
            precision = max(0, len(str(step).rstrip('0').split('.')[-1]))
        else:
            precision = info.get("base_precision", 8)
        return f"{qty:.{precision}f}"

    def _round_price(self, price: float, info: dict) -> str:
        """Round price to symbol's tick size."""
        tick = info.get("tick_size", 0)
        if tick > 0:
            precision = max(0, len(str(tick).rstrip('0').split('.')[-1]))
        else:
            precision = info.get("quote_precision", 8)
        return f"{price:.{precision}f}"

    # ─────────────────────────────────────────────
    # AUTHENTICATED: ACCOUNT
    # ─────────────────────────────────────────────

    async def get_account(self) -> dict:
        """Get spot account info with balances."""
        if not self.api_key:
            raise ValueError("API key required for authenticated endpoints")

        data = await self._signed_get("/api/v3/account")

        balances = {}
        for b in data.get("balances", []):
            free = float(b["free"])
            locked = float(b["locked"])
            if free > 0 or locked > 0:
                balances[b["asset"]] = {
                    "free": free,
                    "locked": locked,
                    "total": round(free + locked, 8),
                }

        return {
            "can_trade": data.get("canTrade", False),
            "balances": balances,
            "usdt_free": balances.get("USDT", {}).get("free", 0),
            "usdt_total": balances.get("USDT", {}).get("total", 0),
        }

    # ─────────────────────────────────────────────
    # AUTHENTICATED: ORDERS
    # ─────────────────────────────────────────────

    async def place_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        order_type: str = "MARKET",
        price: Optional[float] = None,
        time_in_force: str = "GTC",
    ) -> dict:
        """
        Place a spot order (BUY or SELL).

        Args:
            symbol: Trading pair (e.g. BTCUSDT)
            side: "BUY" or "SELL"
            quantity: Amount in base asset
            order_type: "MARKET" or "LIMIT"
            price: Required for LIMIT orders
            time_in_force: For LIMIT orders (GTC, IOC, FOK)

        Returns:
            Order response with orderId, status, fills
        """
        if not self.api_key:
            raise ValueError("API key required for order placement")

        symbol = symbol.upper()
        info = await self.get_symbol_info(symbol)

        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": order_type.upper(),
            "quantity": self._round_qty(quantity, info),
        }

        if order_type.upper() == "LIMIT":
            if price is None:
                raise ValueError("price required for LIMIT orders")
            params["price"] = self._round_price(price, info)
            params["timeInForce"] = time_in_force

        result = await self._signed_post("/api/v3/order", params, retries=3)
        return self._format_order_response(result)

    async def place_quote_order(
        self,
        symbol: str,
        side: str,
        quote_qty: float,
    ) -> dict:
        """
        Place a spot MARKET order specifying the QUOTE amount (USDT).
        Useful when you want to spend exactly X USDT.
        Only works with MARKET orders.
        """
        if not self.api_key:
            raise ValueError("API key required for order placement")

        symbol = symbol.upper()
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quoteOrderQty": f"{quote_qty:.2f}",
        }

        result = await self._signed_post("/api/v3/order", params, retries=3)
        return self._format_order_response(result)

    async def cancel_order(self, symbol: str, order_id: int) -> dict:
        """Cancel a specific open order."""
        result = await self._signed_delete("/api/v3/order", {
            "symbol": symbol.upper(),
            "orderId": order_id,
        })
        return self._format_order_response(result)

    async def get_open_orders(self, symbol: Optional[str] = None) -> list[dict]:
        """Get open orders, optionally filtered by symbol."""
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        data = await self._signed_get("/api/v3/openOrders", params)
        return [self._format_order_response(o) for o in data]

    async def get_my_trades(self, symbol: str, limit: int = 100) -> list[dict]:
        """Get trade history for a symbol."""
        data = await self._signed_get("/api/v3/myTrades", {
            "symbol": symbol.upper(),
            "limit": limit,
        })
        return [
            {
                "id": t["id"],
                "order_id": t["orderId"],
                "price": float(t["price"]),
                "qty": float(t["qty"]),
                "quote_qty": float(t["quoteQty"]),
                "commission": float(t["commission"]),
                "commission_asset": t["commissionAsset"],
                "time": int(t["time"]),
                "is_buyer": t["isBuyer"],
                "is_maker": t["isMaker"],
            }
            for t in data
        ]

    # ─────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────

    def _format_order_response(self, raw: dict) -> dict:
        """Format raw Binance order response."""
        fills = raw.get("fills", [])
        avg_price = 0.0
        total_qty = 0.0
        total_commission = 0.0

        for f in fills:
            qty = float(f.get("qty", 0))
            price = float(f.get("price", 0))
            avg_price += price * qty
            total_qty += qty
            total_commission += float(f.get("commission", 0))

        if total_qty > 0:
            avg_price /= total_qty
        else:
            avg_price = float(raw.get("price", 0))

        return {
            "order_id": raw.get("orderId"),
            "symbol": raw.get("symbol"),
            "side": raw.get("side"),
            "type": raw.get("type"),
            "status": raw.get("status"),
            "price": float(raw.get("price", 0)),
            "avg_price": avg_price,
            "quantity": float(raw.get("origQty", 0)),
            "executed_qty": float(raw.get("executedQty", 0)),
            "cum_quote": float(raw.get("cummulativeQuoteQty", 0)),
            "commission": total_commission,
            "fills": len(fills),
            "time": int(raw.get("transactTime", 0) or raw.get("time", 0)),
        }
