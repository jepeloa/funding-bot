"""
Binance Futures Trading Module
==============================
Authenticated client for placing and managing trades on Binance USDT-M Futures.

Operations:
  - Open position (market / limit, long / short)
  - Close position (full / partial, market / limit)
  - Set TP / SL (take-profit and stop-loss orders)
  - Query open positions with PnL
  - Query account balance
  - Set leverage per symbol

Security:
  - API Key & Secret loaded from .env (BINANCE_API_KEY, BINANCE_API_SECRET)
  - HMAC-SHA256 request signing
  - Every order requires explicit confirmation fields to avoid accidental trades
"""

import hashlib
import hmac
import os
import time
import asyncio
import copy
import urllib.parse
from typing import Optional

import httpx

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

FUTURES_BASE_URL = "https://fapi.binance.com"


class AlgoOrderRequired(Exception):
    """Raised when Binance returns -4120 meaning the order must use the Algo API."""
    pass


def _load_env_file():
    """Load .env file from project root if it exists."""
    # Walk up from this file to find .env
    current = os.path.dirname(os.path.abspath(__file__))
    for _ in range(5):
        env_path = os.path.join(current, ".env")
        if os.path.isfile(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        key, _, value = line.partition("=")
                        key = key.strip()
                        value = value.strip().strip('"').strip("'")
                        os.environ.setdefault(key, value)
            return
        current = os.path.dirname(current)


# Load on import
_load_env_file()


# ─────────────────────────────────────────────
# SIGNED CLIENT
# ─────────────────────────────────────────────

class FuturesTrader:
    """
    Authenticated Binance USDT-M Futures trading client.

    All trading methods require explicit parameters to avoid accidental orders.
    Uses HMAC-SHA256 signing for all authenticated endpoints.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self.api_key = api_key or os.environ.get("BINANCE_API_KEY", "")
        self.api_secret = api_secret or os.environ.get("BINANCE_API_SECRET", "")

        if not self.api_key or not self.api_secret:
            raise ValueError(
                "BINANCE_API_KEY and BINANCE_API_SECRET must be set. "
                "Add them to .env file or set as environment variables."
            )

        self.client = httpx.AsyncClient(
            base_url=FUTURES_BASE_URL,
            timeout=timeout,
            headers={
                "X-MBX-APIKEY": self.api_key,
                "User-Agent": "PsiJamMCP/0.1",
            },
        )

        # Cache for exchange info (precision, filters)
        self._symbol_info_cache: dict = {}

    async def close(self):
        await self.client.aclose()

    # ─────────────────────────────────────────────
    # HEDGE MODE HELPERS
    # ─────────────────────────────────────────────

    @staticmethod
    def _is_hedge_position(position: dict) -> bool:
        """Check if a single position entry is from hedge mode.
        In hedge mode positionSide is 'LONG' or 'SHORT'.
        In one-way mode positionSide is 'BOTH'.
        """
        return position.get("position_side", "BOTH") in ("LONG", "SHORT")

    async def _detect_hedge_mode(self) -> bool:
        """Detect hedge mode via API. Used only for open_position (no position data yet)."""
        try:
            data = await self._signed_get("/fapi/v1/positionSide/dual")
            return data.get("dualSidePosition", False)
        except Exception:
            return False

    # ─────────────────────────────────────────────
    # SIGNING
    # ─────────────────────────────────────────────

    def _sign(self, params: dict) -> dict:
        """Add timestamp, recvWindow, and HMAC-SHA256 signature to request params."""
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
        """Signed POST request with optional retry for transient Binance errors.

        Args:
            path: API endpoint path
            params: Request parameters (will be signed)
            retries: Number of retries for transient errors. Default 0.
                     Use retries=3 for order placement calls.

        Raises:
            AlgoOrderRequired: When Binance returns -4120 (STOP_ORDER_SWITCH_ALGO),
                meaning this conditional order type must use the Algo Order API.
            httpx.HTTPStatusError: For other HTTP errors.
        """
        signed = self._sign(copy.deepcopy(params or {}))
        resp = await self.client.post(path, params=signed)

        # Check for -4120 (STOP_ORDER_SWITCH_ALGO) — permanent, not transient.
        # Since Dec 2025, conditional orders (STOP_MARKET, TAKE_PROFIT_MARKET, etc.)
        # must use POST /fapi/v1/algoOrder instead of POST /fapi/v1/order.
        if resp.status_code == 400:
            try:
                body = resp.json()
            except Exception:
                body = {}
            if body.get("code") == -4120:
                raise AlgoOrderRequired(body.get("msg", "STOP_ORDER_SWITCH_ALGO"))

        # Retry logic for other transient errors (e.g., -1008 overload)
        if retries > 0 and resp.status_code >= 400:
            original_params = params or {}
            for attempt in range(retries):
                await asyncio.sleep(0.15 * (attempt + 1))
                signed = self._sign(copy.deepcopy(original_params))
                resp = await self.client.post(path, params=signed)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 400:
                    try:
                        body = resp.json()
                    except Exception:
                        break
                    if body.get("code") == -4120:
                        raise AlgoOrderRequired(body.get("msg", "STOP_ORDER_SWITCH_ALGO"))

        resp.raise_for_status()
        return resp.json()

    async def _signed_delete(self, path: str, params: dict = None) -> dict:
        """Signed DELETE request."""
        params = self._sign(params or {})
        resp = await self.client.delete(path, params=params)
        resp.raise_for_status()
        return resp.json()

    # ─────────────────────────────────────────────
    # ALGO ORDERS (conditional: STOP_MARKET, TP_MARKET, etc.)
    # Since Dec 2025 Binance migrated conditional orders to POST /fapi/v1/algoOrder
    # ─────────────────────────────────────────────

    async def _place_algo_order(self, params: dict) -> dict:
        """Place a conditional order via the Algo Order API.

        Translates standard order params to algo params:
          - Adds algoType=CONDITIONAL
          - Renames stopPrice → triggerPrice
          - Renames activationPrice → activationPrice (kept as-is for trailing)
          - Keeps callbackRate for TRAILING_STOP_MARKET

        Returns the raw Binance algo order response.
        """
        algo_params = dict(params)

        # Required: algoType
        algo_params["algoType"] = "CONDITIONAL"

        # Rename stopPrice → triggerPrice (algo API naming)
        if "stopPrice" in algo_params:
            algo_params["triggerPrice"] = algo_params.pop("stopPrice")

        # Algo API doesn't accept these legacy params  
        algo_params.pop("newOrderRespType", None)

        signed = self._sign(copy.deepcopy(algo_params))
        resp = await self.client.post("/fapi/v1/algoOrder", params=signed)
        resp.raise_for_status()
        return resp.json()

    def _format_algo_order_response(self, raw: dict) -> dict:
        """Format raw Binance algo order response into a dict compatible with
        our standard order format."""
        return {
            "order_id": raw.get("algoId"),
            "client_order_id": raw.get("clientAlgoId"),
            "symbol": raw.get("symbol"),
            "side": raw.get("side"),
            "type": raw.get("orderType"),
            "status": raw.get("algoStatus"),
            "price": float(raw.get("price", 0) or 0),
            "avg_price": 0,
            "stop_price": float(raw.get("triggerPrice", 0) or 0),
            "quantity": float(raw.get("quantity", 0) or 0),
            "executed_qty": 0,
            "cum_quote_usdt": 0,
            "reduce_only": raw.get("reduceOnly", False),
            "close_position": raw.get("closePosition", False),
            "time": int(raw.get("createTime", 0)),
            "algo_order": True,
            "algo_type": raw.get("algoType"),
            "working_type": raw.get("workingType", ""),
            "price_protect": raw.get("priceProtect", False),
        }

    async def get_open_algo_orders(self, symbol: Optional[str] = None) -> dict:
        """Get all open algo (conditional) orders, optionally filtered by symbol."""
        params: dict = {}
        if symbol:
            params["symbol"] = symbol.upper()
        data = await self._signed_get("/fapi/v1/openAlgoOrders", params)

        orders_list = data if isinstance(data, list) else data.get("orders", data)
        orders = []
        for o in (orders_list if isinstance(orders_list, list) else []):
            orders.append({
                "order_id": o.get("algoId"),
                "client_order_id": o.get("clientAlgoId"),
                "symbol": o.get("symbol"),
                "side": o.get("side"),
                "type": o.get("orderType"),
                "price": float(o.get("price", 0) or 0),
                "stop_price": float(o.get("triggerPrice", 0) or 0),
                "quantity": float(o.get("quantity", 0) or 0),
                "executed_qty": 0,
                "status": o.get("algoStatus"),
                "time_in_force": o.get("timeInForce", ""),
                "reduce_only": o.get("reduceOnly", False),
                "close_position": o.get("closePosition", False),
                "working_type": o.get("workingType", ""),
                "time": int(o.get("createTime", 0)),
                "algo_order": True,
            })

        return {"count": len(orders), "orders": orders}

    async def cancel_algo_order(self, algo_id: int) -> dict:
        """Cancel a specific open algo order by algoId."""
        result = await self._signed_delete("/fapi/v1/algoOrder", {
            "algoId": algo_id,
        })
        return {
            "algo_id": result.get("algoId"),
            "client_algo_id": result.get("clientAlgoId"),
            "code": result.get("code"),
            "msg": result.get("msg"),
        }

    async def cancel_all_algo_orders(self, symbol: str) -> dict:
        """Cancel ALL open algo orders for a symbol."""
        symbol = symbol.upper()
        result = await self._signed_delete("/fapi/v1/algoOpenOrders", {
            "symbol": symbol,
        })
        return {"symbol": symbol, "result": result}

    # ─────────────────────────────────────────────
    # SYMBOL INFO (precision, filters)
    # ─────────────────────────────────────────────

    async def _get_symbol_info(self, symbol: str) -> dict:
        """Get symbol precision and filter info, cached."""
        symbol = symbol.upper()
        if symbol in self._symbol_info_cache:
            return self._symbol_info_cache[symbol]

        resp = await self.client.get("/fapi/v1/exchangeInfo")
        resp.raise_for_status()
        for s in resp.json().get("symbols", []):
            sym = s["symbol"]
            info = {
                "symbol": sym,
                "price_precision": s.get("pricePrecision", 2),
                "quantity_precision": s.get("quantityPrecision", 3),
                "base_asset": s.get("baseAsset", ""),
                "quote_asset": s.get("quoteAsset", ""),
                "filters": {f["filterType"]: f for f in s.get("filters", [])},
            }
            self._symbol_info_cache[sym] = info

        if symbol not in self._symbol_info_cache:
            raise ValueError(f"Symbol {symbol} not found on Binance Futures")

        return self._symbol_info_cache[symbol]

    def _round_price(self, price: float, info: dict) -> str:
        """Round price to symbol's precision."""
        precision = info["price_precision"]
        return f"{price:.{precision}f}"

    def _round_qty(self, qty: float, info: dict) -> str:
        """Round quantity to symbol's precision."""
        precision = info["quantity_precision"]
        return f"{qty:.{precision}f}"

    # ─────────────────────────────────────────────
    # ACCOUNT INFO
    # ─────────────────────────────────────────────

    async def get_account_balance(self) -> dict:
        """
        Get futures account balance.
        Returns USDT balance with available, total, and unrealized PnL.
        """
        data = await self._signed_get("/fapi/v2/balance")

        balances = {}
        for b in data:
            asset = b["asset"]
            balance = float(b["balance"])
            if balance > 0 or asset == "USDT":
                balances[asset] = {
                    "balance": round(balance, 4),
                    "available": round(float(b["availableBalance"]), 4),
                    "cross_wallet": round(float(b.get("crossWalletBalance", 0)), 4),
                    "cross_unrealized_pnl": round(float(b.get("crossUnPnl", 0)), 4),
                }

        usdt = balances.get("USDT", {})
        return {
            "usdt_balance": usdt.get("balance", 0),
            "usdt_available": usdt.get("available", 0),
            "usdt_unrealized_pnl": usdt.get("cross_unrealized_pnl", 0),
            "all_assets": balances,
        }

    async def get_positions(self, symbol: Optional[str] = None) -> dict:
        """
        Get open positions with PnL, leverage, entry price, liquidation price.
        If symbol is given, returns only that position.
        Supports both one-way and hedge mode (auto-detected from positionSide field).
        """
        data = await self._signed_get("/fapi/v2/positionRisk")

        positions = []
        detected_hedge = False

        for p in data:
            amt = float(p.get("positionAmt", 0))
            position_side = p.get("positionSide", "BOTH")  # BOTH (one-way), LONG, SHORT (hedge)

            # Track if this account uses hedge mode
            if position_side in ("LONG", "SHORT"):
                detected_hedge = True

            # Filter by symbol if specified
            if symbol and p["symbol"] != symbol.upper():
                continue

            # Skip empty positions (amt == 0)
            if amt == 0:
                continue

            entry_price = float(p.get("entryPrice", 0))
            mark_price = float(p.get("markPrice", 0))
            pnl = float(p.get("unRealizedProfit", 0))
            leverage = int(p.get("leverage", 1))
            liq_price = float(p.get("liquidationPrice", 0))
            notional = float(p.get("notional", 0))

            # Direction: use positionSide if hedge, else infer from amount
            if position_side in ("LONG", "SHORT"):
                side = position_side
            elif amt > 0:
                side = "LONG"
            elif amt < 0:
                side = "SHORT"
            else:
                side = "NONE"

            # ROE (return on equity)
            margin = abs(notional) / leverage if leverage > 0 else 0
            roe_pct = (pnl / margin * 100) if margin > 0 else 0

            # Distance to liquidation
            liq_distance_pct = 0
            if liq_price > 0 and mark_price > 0:
                liq_distance_pct = abs(mark_price - liq_price) / mark_price * 100

            positions.append({
                "symbol": p["symbol"],
                "side": side,
                "position_side": position_side,  # BOTH, LONG, or SHORT  (raw from Binance)
                "size": abs(amt),
                "size_raw": amt,
                "entry_price": entry_price,
                "mark_price": mark_price,
                "notional_usdt": round(abs(notional), 2),
                "leverage": leverage,
                "margin_type": p.get("marginType", "cross"),
                "unrealized_pnl": round(pnl, 4),
                "roe_pct": round(roe_pct, 2),
                "liquidation_price": liq_price,
                "liq_distance_pct": round(liq_distance_pct, 2),
            })

        return {
            "count": len(positions),
            "positions": positions,
            "hedge_mode": detected_hedge,
        }

    # ─────────────────────────────────────────────
    # LEVERAGE
    # ─────────────────────────────────────────────

    async def set_leverage(self, symbol: str, leverage: int) -> dict:
        """
        Set leverage for a symbol (1-125).
        Must be set before opening a position.
        """
        symbol = symbol.upper()
        leverage = max(1, min(125, leverage))

        result = await self._signed_post("/fapi/v1/leverage", {
            "symbol": symbol,
            "leverage": leverage,
        })

        return {
            "symbol": result.get("symbol", symbol),
            "leverage": int(result.get("leverage", leverage)),
            "max_notional": float(result.get("maxNotionalValue", 0)),
        }

    # ─────────────────────────────────────────────
    # OPEN POSITION
    # ─────────────────────────────────────────────

    async def open_position(
        self,
        symbol: str,
        side: str,
        quantity: float,
        order_type: str = "MARKET",
        price: Optional[float] = None,
        leverage: Optional[int] = None,
        time_in_force: str = "GTC",
        take_profit: Optional[float] = None,
        stop_loss: Optional[float] = None,
        trailing_stop_callback: Optional[float] = None,
        trailing_stop_activation: Optional[float] = None,
    ) -> dict:
        """
        Open a new position (LONG or SHORT).
        Auto-detects hedge mode and includes positionSide if needed.
        Optionally sets TP, SL, and/or trailing stop at the same time.

        Args:
            symbol: Trading pair (e.g. BTCUSDT)
            side: "LONG" or "SHORT"
            quantity: Amount in base asset
            order_type: "MARKET" or "LIMIT"
            price: Required for LIMIT orders
            leverage: If set, adjusts leverage before opening
            time_in_force: For LIMIT orders (GTC, IOC, FOK)
            take_profit: TP trigger price (optional)
            stop_loss: SL trigger price (optional)
            trailing_stop_callback: Callback rate % for trailing stop (0.1–5.0, optional)
            trailing_stop_activation: Activation price for trailing stop (optional)

        Returns:
            Order response with orderId, status, fills, and TP/SL/trailing results if requested
        """
        symbol = symbol.upper()
        side_upper = side.upper()

        if side_upper not in ("LONG", "SHORT"):
            raise ValueError("side must be 'LONG' or 'SHORT'")

        # Detect hedge mode (need API call since we don't have position data yet)
        is_hedge = await self._detect_hedge_mode()

        # Map to Binance order side: LONG = BUY, SHORT = SELL
        order_side = "BUY" if side_upper == "LONG" else "SELL"

        # Set leverage if requested
        leverage_info = None
        if leverage is not None:
            leverage_info = await self.set_leverage(symbol, leverage)

        # Get symbol info for precision
        info = await self._get_symbol_info(symbol)

        params = {
            "symbol": symbol,
            "side": order_side,
            "type": order_type.upper(),
            "quantity": self._round_qty(quantity, info),
        }

        # Hedge mode: add positionSide
        if is_hedge:
            params["positionSide"] = side_upper  # LONG or SHORT

        if order_type.upper() == "LIMIT":
            if price is None:
                raise ValueError("price is required for LIMIT orders")
            params["price"] = self._round_price(price, info)
            params["timeInForce"] = time_in_force

        result = await self._signed_post("/fapi/v1/order", params, retries=5)

        formatted = self._format_order_response(result, leverage_info=leverage_info)

        # ── Place TP/SL/Trailing if requested ──
        if take_profit is not None or stop_loss is not None or trailing_stop_callback is not None:
            order_status = formatted.get("status", "")

            if order_status == "FILLED":
                # MARKET order filled immediately → use set_tp_sl (full position validation)
                tp_sl_result = await self.set_tp_sl(
                    symbol=symbol,
                    take_profit=take_profit,
                    stop_loss=stop_loss,
                    trailing_stop_callback=trailing_stop_callback,
                    trailing_stop_activation=trailing_stop_activation,
                )
                formatted["tp_sl"] = tp_sl_result
            else:
                # LIMIT order (NEW/PARTIALLY_FILLED) → pre-place TP/SL
                # Use the limit price as expected entry for validation
                expected_entry = price if price else formatted.get("price", 0)
                tp_sl_result = await self._place_tp_sl_for_entry(
                    symbol=symbol,
                    side=side_upper,
                    quantity=quantity,
                    expected_entry=expected_entry,
                    take_profit=take_profit,
                    stop_loss=stop_loss,
                    is_hedge=is_hedge,
                    info=info,
                )
                formatted["tp_sl"] = tp_sl_result

        return formatted

    # ─────────────────────────────────────────────
    # STOP-LIMIT ENTRY ORDER
    # ─────────────────────────────────────────────

    async def open_stop_limit_position(
        self,
        symbol: str,
        side: str,
        quantity: float,
        stop_price: float,
        price: float,
        leverage: Optional[int] = None,
        time_in_force: str = "GTC",
        take_profit: Optional[float] = None,
        stop_loss: Optional[float] = None,
    ) -> dict:
        """
        Open a STOP-LIMIT entry order.
        The order activates when mark price reaches stop_price, then places
        a limit order at 'price'.

        Args:
            symbol: Trading pair (e.g. BTCUSDT)
            side: "LONG" or "SHORT"
            quantity: Amount in base asset
            stop_price: Trigger price (when reached, limit order activates)
            price: Limit price for the order after trigger
            leverage: Set leverage before opening (optional)
            time_in_force: GTC, IOC, FOK
            take_profit: TP trigger price (optional, pre-placed)
            stop_loss: SL trigger price (optional, pre-placed)

        Returns:
            Order response with orderId, status, and TP/SL if requested
        """
        symbol = symbol.upper()
        side_upper = side.upper()

        if side_upper not in ("LONG", "SHORT"):
            raise ValueError("side must be 'LONG' or 'SHORT'")

        # Detect hedge mode
        is_hedge = await self._detect_hedge_mode()

        # Map to Binance order side: LONG = BUY, SHORT = SELL
        order_side = "BUY" if side_upper == "LONG" else "SELL"

        # Set leverage if requested
        leverage_info = None
        if leverage is not None:
            leverage_info = await self.set_leverage(symbol, leverage)

        # Get symbol info for precision
        info = await self._get_symbol_info(symbol)

        # Validate price vs stop_price direction
        # LONG (BUY): stop_price triggers above current → price should be near/above stop
        # SHORT (SELL): stop_price triggers below current → price should be near/below stop

        params = {
            "symbol": symbol,
            "side": order_side,
            "type": "STOP",
            "quantity": self._round_qty(quantity, info),
            "price": self._round_price(price, info),
            "stopPrice": self._round_price(stop_price, info),
            "timeInForce": time_in_force,
            "workingType": "MARK_PRICE",
        }

        # Hedge mode: add positionSide
        if is_hedge:
            params["positionSide"] = side_upper

        # Try standard endpoint first, fallback to algo on -4120
        try:
            result = await self._signed_post("/fapi/v1/order", params, retries=5)
            formatted = self._format_order_response(result, leverage_info=leverage_info)
        except AlgoOrderRequired:
            result = await self._place_algo_order(params)
            formatted = self._format_algo_order_response(result)
            formatted["note"] = "Placed via Algo Order API (conditional order migration)"
            if leverage_info:
                formatted["leverage_set"] = leverage_info

        formatted["order_subtype"] = "STOP_LIMIT_ENTRY"
        formatted["trigger_price"] = stop_price
        formatted["limit_price"] = price

        # ── Place TP/SL if requested ──
        if take_profit is not None or stop_loss is not None:
            tp_sl_result = await self._place_tp_sl_for_entry(
                symbol=symbol,
                side=side_upper,
                quantity=quantity,
                expected_entry=price,  # Use limit price as expected entry
                take_profit=take_profit,
                stop_loss=stop_loss,
                is_hedge=is_hedge,
                info=info,
            )
            formatted["tp_sl"] = tp_sl_result

        return formatted

    # ─────────────────────────────────────────────
    # TP/SL FOR NEW ENTRIES (before position exists)
    # ─────────────────────────────────────────────

    async def _place_tp_sl_for_entry(
        self,
        symbol: str,
        side: str,
        quantity: float,
        expected_entry: float,
        take_profit: Optional[float] = None,
        stop_loss: Optional[float] = None,
        is_hedge: bool = False,
        info: Optional[dict] = None,
    ) -> dict:
        """
        Pre-place TP/SL conditional orders for a pending entry (LIMIT or STOP_LIMIT).
        These orders will activate once the entry fills and price reaches their triggers.

        Uses expected_entry (the limit price) for price direction validation.

        Args:
            symbol: Trading pair
            side: "LONG" or "SHORT"
            quantity: Position quantity
            expected_entry: Expected entry price (limit price)
            take_profit: TP trigger price
            stop_loss: SL trigger price
            is_hedge: Whether account is in hedge mode
            info: Symbol precision info (fetched if not provided)
        """
        if info is None:
            info = await self._get_symbol_info(symbol)

        # Close side (opposite of position)
        close_side = "SELL" if side == "LONG" else "BUY"

        # ── Price direction validation ──
        validation_errors = []
        if take_profit is not None:
            if side == "LONG" and take_profit <= expected_entry:
                validation_errors.append(
                    f"TP ({take_profit}) debe ser MAYOR que entry esperado ({expected_entry}) para LONG"
                )
            elif side == "SHORT" and take_profit >= expected_entry:
                validation_errors.append(
                    f"TP ({take_profit}) debe ser MENOR que entry esperado ({expected_entry}) para SHORT"
                )

        if stop_loss is not None:
            if side == "LONG" and stop_loss >= expected_entry:
                validation_errors.append(
                    f"SL ({stop_loss}) debe ser MENOR que entry esperado ({expected_entry}) para LONG"
                )
            elif side == "SHORT" and stop_loss <= expected_entry:
                validation_errors.append(
                    f"SL ({stop_loss}) debe ser MAYOR que entry esperado ({expected_entry}) para SHORT"
                )

        if validation_errors:
            return {
                "error": "Validación de precio TP/SL fallida",
                "validation_errors": validation_errors,
                "expected_entry": expected_entry,
                "side": side,
                "hint": (
                    "LONG → TP por encima del entry, SL por debajo del entry. "
                    "SHORT → TP por debajo del entry, SL por encima del entry."
                ),
            }

        # Determine positionSide for hedge mode
        position_side = side if is_hedge else None  # LONG or SHORT

        def _build_params(order_type: str, trigger_price: float) -> dict:
            p = {
                "symbol": symbol,
                "side": close_side,
                "type": order_type,
                "quantity": self._round_qty(quantity, info),
                "stopPrice": self._round_price(trigger_price, info),
                "workingType": "MARK_PRICE",
                "priceProtect": "true",
            }
            if is_hedge and position_side:
                p["positionSide"] = position_side
            else:
                p["reduceOnly"] = "true"
            return p

        async def _place_conditional(order_type: str, trigger_price: float) -> dict:
            params = _build_params(order_type, trigger_price)
            try:
                r = await self._signed_post("/fapi/v1/order", params, retries=3)
                return self._format_order_response(r)
            except AlgoOrderRequired:
                try:
                    r = await self._place_algo_order(params)
                    res = self._format_algo_order_response(r)
                    res["note"] = "Placed via Algo Order API (conditional order migration)"
                    return res
                except httpx.HTTPStatusError as e2:
                    try:
                        body = e2.response.json()
                        return {"error": body.get("msg", str(e2)), "code": body.get("code")}
                    except Exception:
                        return {"error": str(e2)}
            except httpx.HTTPStatusError as e:
                try:
                    body = e.response.json()
                    return {"error": body.get("msg", str(e)), "code": body.get("code")}
                except Exception:
                    return {"error": str(e)}

        result = {"pre_placed": True, "expected_entry": expected_entry}

        # Place TP and SL (parallel if both)
        if take_profit is not None and stop_loss is not None:
            tp_res, sl_res = await asyncio.gather(
                _place_conditional("TAKE_PROFIT_MARKET", take_profit),
                _place_conditional("STOP_MARKET", stop_loss),
            )
            result["tp_order"] = tp_res
            result["sl_order"] = sl_res

            tp_ok = "error" not in tp_res
            sl_ok = "error" not in sl_res
            if tp_ok and not sl_ok:
                result["warning"] = "TP pre-colocado OK pero SL FALLÓ. Revisar SL."
            elif sl_ok and not tp_ok:
                result["warning"] = "SL pre-colocado OK pero TP FALLÓ. Revisar TP."
            elif not tp_ok and not sl_ok:
                result["warning"] = "Ambos (TP y SL) FALLARON. Verificar precios y reintentar."
        elif take_profit is not None:
            result["tp_order"] = await _place_conditional("TAKE_PROFIT_MARKET", take_profit)
        elif stop_loss is not None:
            result["sl_order"] = await _place_conditional("STOP_MARKET", stop_loss)

        result["note"] = (
            "TP/SL pre-colocados. Se activarán cuando la posición se abra y el precio alcance los triggers."
        )
        return result

    # ─────────────────────────────────────────────
    # CLOSE POSITION
    # ─────────────────────────────────────────────

    async def close_position(
        self,
        symbol: str,
        quantity: Optional[float] = None,
        order_type: str = "MARKET",
        price: Optional[float] = None,
        time_in_force: str = "GTC",
    ) -> dict:
        """
        Close an open position (full or partial).
        Auto-detects hedge mode and uses positionSide instead of reduceOnly.

        Args:
            symbol: Trading pair
            quantity: Amount to close (None = close full position)
            order_type: "MARKET" or "LIMIT"
            price: Required for LIMIT closes
            time_in_force: For LIMIT orders

        Returns:
            Order response
        """
        symbol = symbol.upper()

        # Get current position to determine side and size
        pos_data = await self.get_positions(symbol)
        positions = pos_data.get("positions", [])

        # Only active positions
        active = [p for p in positions if p["side"] != "NONE" and p["size"] > 0]

        if not active:
            return {"error": f"No open position for {symbol}"}

        pos = active[0]
        is_hedge = self._is_hedge_position(pos)

        # Close opposite to current position
        close_side = "SELL" if pos["side"] == "LONG" else "BUY"

        # Determine quantity
        info = await self._get_symbol_info(symbol)
        if quantity is None:
            close_qty = pos["size"]
        else:
            close_qty = min(quantity, pos["size"])

        params = {
            "symbol": symbol,
            "side": close_side,
            "type": order_type.upper(),
            "quantity": self._round_qty(close_qty, info),
        }

        # Hedge mode: use positionSide (reduceOnly is NOT allowed in hedge mode)
        if is_hedge:
            params["positionSide"] = pos["position_side"]  # LONG or SHORT
        else:
            params["reduceOnly"] = "true"

        if order_type.upper() == "LIMIT":
            if price is None:
                raise ValueError("price is required for LIMIT closes")
            params["price"] = self._round_price(price, info)
            params["timeInForce"] = time_in_force

        result = await self._signed_post("/fapi/v1/order", params, retries=5)

        return self._format_order_response(result, closing=True, original_position=pos)

    # ─────────────────────────────────────────────
    # TP / SL
    # ─────────────────────────────────────────────

    async def set_tp_sl(
        self,
        symbol: str,
        take_profit: Optional[float] = None,
        stop_loss: Optional[float] = None,
        tp_quantity: Optional[float] = None,
        sl_quantity: Optional[float] = None,
        trailing_stop_callback: Optional[float] = None,
        trailing_stop_activation: Optional[float] = None,
        trailing_stop_quantity: Optional[float] = None,
        take_profits: Optional[list] = None,
    ) -> dict:
        """
        Set take-profit and/or stop-loss for an open position.
        Optionally places a TRAILING_STOP_MARKET order.
        Supports multiple TP levels via take_profits parameter.
        Works in both one-way and hedge mode.

        Args:
            symbol: Trading pair
            take_profit: Single TP price level (ignored if take_profits is provided)
            stop_loss: SL price level
            tp_quantity: Qty for single TP (None = full position via closePosition)
            sl_quantity: Qty for SL (None = full position via closePosition)
            trailing_stop_callback: Callback rate % for trailing stop (0.1 – 5.0)
            trailing_stop_activation: Activation price for trailing (optional)
            trailing_stop_quantity: Qty for trailing stop (None = full position)
            take_profits: List of TP levels, each dict with:
                          - "price": trigger price
                          - "quantity_pct": % of position to close (1-100)
                          Example: [{"price": 2000, "quantity_pct": 50},
                                    {"price": 2100, "quantity_pct": 30}]

        Returns:
            Dict with tp_order(s), sl_order, and trailing_order results
        """
        symbol = symbol.upper()

        has_tp = take_profit is not None or take_profits
        if not has_tp and stop_loss is None and trailing_stop_callback is None:
            return {"error": "At least one of take_profit, take_profits, stop_loss, or trailing_stop_callback must be specified"}

        # Get current position
        pos_data = await self.get_positions(symbol)
        positions = pos_data.get("positions", [])

        # Only active positions
        active = [p for p in positions if p["side"] != "NONE" and p["size"] > 0]

        if not active:
            return {"error": f"No open position for {symbol}"}

        pos = active[0]
        is_hedge = self._is_hedge_position(pos)
        side = pos["side"]          # LONG or SHORT
        entry = pos["entry_price"]
        mark = pos["mark_price"]

        info = await self._get_symbol_info(symbol)

        # ── Price direction validation ──
        # LONG: TP must be > entry, SL must be < mark price
        # SHORT: TP must be < entry, SL must be > mark price
        validation_errors = []

        if take_profit is not None:
            if side == "LONG" and take_profit <= entry:
                validation_errors.append(
                    f"TP ({take_profit}) debe ser MAYOR que entry ({entry}) para posición LONG"
                )
            elif side == "SHORT" and take_profit >= entry:
                validation_errors.append(
                    f"TP ({take_profit}) debe ser MENOR que entry ({entry}) para posición SHORT"
                )

        if take_profits:
            total_pct = 0
            for i, tp_level in enumerate(take_profits):
                tp_price = tp_level.get("price")
                tp_pct = tp_level.get("quantity_pct", 0)
                if tp_price is None:
                    validation_errors.append(f"take_profits[{i}]: falta 'price'")
                    continue
                if tp_pct <= 0 or tp_pct > 100:
                    validation_errors.append(f"take_profits[{i}]: quantity_pct ({tp_pct}) debe ser 1-100")
                total_pct += tp_pct
                if side == "LONG" and tp_price <= entry:
                    validation_errors.append(
                        f"take_profits[{i}]: precio ({tp_price}) debe ser MAYOR que entry ({entry}) para LONG"
                    )
                elif side == "SHORT" and tp_price >= entry:
                    validation_errors.append(
                        f"take_profits[{i}]: precio ({tp_price}) debe ser MENOR que entry ({entry}) para SHORT"
                    )
            if total_pct > 100:
                validation_errors.append(
                    f"take_profits: suma de quantity_pct ({total_pct}%) excede 100%"
                )

        if stop_loss is not None:
            if side == "LONG" and stop_loss >= mark:
                validation_errors.append(
                    f"SL ({stop_loss}) debe ser MENOR que mark price ({mark}) para posición LONG"
                )
            elif side == "SHORT" and stop_loss <= mark:
                validation_errors.append(
                    f"SL ({stop_loss}) debe ser MAYOR que mark price ({mark}) para posición SHORT"
                )

        if trailing_stop_callback is not None:
            if trailing_stop_callback < 0.1 or trailing_stop_callback > 5.0:
                validation_errors.append(
                    f"Trailing callback ({trailing_stop_callback}%) fuera de rango. Binance permite 0.1% – 5.0%."
                )

        if validation_errors:
            return {
                "error": "Validación de precio fallida",
                "validation_errors": validation_errors,
                "position": pos,
                "hint": (
                    "LONG → TP por encima del entry, SL por debajo del mark price. "
                    "SHORT → TP por debajo del entry, SL por encima del mark price."
                ),
            }

        # TP/SL close side (opposite of position)
        close_side = "SELL" if side == "LONG" else "BUY"

        result = {}

        def _parse_binance_error(exc: httpx.HTTPStatusError) -> dict:
            """Extract Binance error code/msg from HTTP error response."""
            try:
                body = exc.response.json()
                return {
                    "error": body.get("msg", str(exc)),
                    "code": body.get("code"),
                    "detail": body.get("msg", exc.response.text),
                }
            except Exception:
                return {"error": str(exc), "detail": exc.response.text}

        # ── Helper: build params for conditional order ──
        def _build_conditional_params(
            order_type: str, stop_price: float, qty: Optional[float],
        ) -> dict:
            """Build order params for TP or SL (works for both standard and algo)."""
            params = {
                "symbol": symbol,
                "side": close_side,
                "type": order_type,
                "stopPrice": self._round_price(stop_price, info),
                "workingType": "MARK_PRICE",
                "priceProtect": "true",
            }
            if qty is not None:
                params["quantity"] = self._round_qty(qty, info)
                if is_hedge:
                    params["positionSide"] = pos["position_side"]
                else:
                    params["reduceOnly"] = "true"
            else:
                params["closePosition"] = "true"
                if is_hedge:
                    params["positionSide"] = pos["position_side"]
            return params

        async def _place_conditional(order_type: str, stop_price: float, qty: Optional[float]) -> dict:
            """Place a conditional order. Tries standard endpoint first;
            on -4120 (STOP_ORDER_SWITCH_ALGO) falls back to Algo Order API."""
            params = _build_conditional_params(order_type, stop_price, qty)
            try:
                r = await self._signed_post("/fapi/v1/order", params, retries=3)
                return self._format_order_response(r)
            except AlgoOrderRequired:
                # Binance requires Algo Order API for this symbol/type
                try:
                    r = await self._place_algo_order(params)
                    res = self._format_algo_order_response(r)
                    res["note"] = "Placed via Algo Order API (conditional order migration)"
                    return res
                except httpx.HTTPStatusError as e2:
                    return _parse_binance_error(e2)
            except httpx.HTTPStatusError as e:
                return _parse_binance_error(e)

        # ── Helper: build params for TRAILING_STOP_MARKET ──
        def _build_trailing_params(callback_rate: float, activation_price: Optional[float], qty: Optional[float]) -> dict:
            params = {
                "symbol": symbol,
                "side": close_side,
                "type": "TRAILING_STOP_MARKET",
                "callbackRate": str(callback_rate),
                "workingType": "MARK_PRICE",
                "priceProtect": "true",
            }
            if activation_price is not None:
                params["activationPrice"] = self._round_price(activation_price, info)
            if qty is not None:
                params["quantity"] = self._round_qty(qty, info)
                if is_hedge:
                    params["positionSide"] = pos["position_side"]
                else:
                    params["reduceOnly"] = "true"
            else:
                params["closePosition"] = "true"
                if is_hedge:
                    params["positionSide"] = pos["position_side"]
            return params

        async def _place_trailing() -> dict:
            """Place a TRAILING_STOP_MARKET order."""
            params = _build_trailing_params(
                trailing_stop_callback, trailing_stop_activation, trailing_stop_quantity,
            )
            try:
                r = await self._signed_post("/fapi/v1/order", params, retries=3)
                return self._format_order_response(r)
            except AlgoOrderRequired:
                try:
                    r = await self._place_algo_order(params)
                    res = self._format_algo_order_response(r)
                    res["note"] = "Placed via Algo Order API (conditional order migration)"
                    return res
                except httpx.HTTPStatusError as e2:
                    return _parse_binance_error(e2)
            except httpx.HTTPStatusError as e:
                return _parse_binance_error(e)

        # Build order coroutines for parallel execution
        async def _place_tp():
            return await _place_conditional("TAKE_PROFIT_MARKET", take_profit, tp_quantity)

        async def _place_sl():
            return await _place_conditional("STOP_MARKET", stop_loss, sl_quantity)

        # Collect coroutines to run in parallel
        coros = []
        coro_keys = []

        # Multiple TPs (take_profits array) takes precedence over single take_profit
        if take_profits:
            position_size = pos["size"]
            for i, tp_level in enumerate(take_profits):
                tp_price = tp_level["price"]
                tp_pct = tp_level["quantity_pct"]
                tp_qty = round(position_size * (tp_pct / 100), 8)
                # Each TP level gets its own coroutine with explicit quantity
                coros.append(_place_conditional("TAKE_PROFIT_MARKET", tp_price, tp_qty))
                coro_keys.append(f"tp_order_{i + 1}")
        elif take_profit is not None:
            coros.append(_place_tp())
            coro_keys.append("tp_order")

        if stop_loss is not None:
            coros.append(_place_sl())
            coro_keys.append("sl_order")
        if trailing_stop_callback is not None:
            coros.append(_place_trailing())
            coro_keys.append("trailing_order")

        # Execute all legs in parallel
        if coros:
            results_list = await asyncio.gather(*coros)
            for key, res in zip(coro_keys, results_list):
                result[key] = res

        # Warn about partial failures
        warnings = []
        for key in coro_keys:
            if key in result and "error" in result[key]:
                label = key.replace("_order", "").replace("_", " ").upper()
                warnings.append(f"{label} FALLÓ")

        ok_count = len(coro_keys) - len(warnings)
        if warnings and ok_count > 0:
            result["warning"] = (
                f"{'  '.join(warnings)}. "
                f"{ok_count}/{len(coro_keys)} órdenes colocadas OK. Revisar las fallidas."
            )
        elif warnings and ok_count == 0:
            result["warning"] = (
                "Todas las órdenes FALLARON. "
                "La posición NO tiene protección. Verificar precios y reintentar."
            )

        result["position"] = pos
        result["hedge_mode"] = is_hedge
        return result

    # ─────────────────────────────────────────────
    # OPEN ORDERS
    # ─────────────────────────────────────────────

    async def get_open_orders(self, symbol: Optional[str] = None) -> dict:
        """Get all open orders (standard + algo/conditional), optionally filtered by symbol."""
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()

        # Standard orders
        data = await self._signed_get("/fapi/v1/openOrders", params)

        orders = []
        for o in data:
            orders.append({
                "order_id": o["orderId"],
                "symbol": o["symbol"],
                "side": o["side"],
                "type": o["type"],
                "price": float(o.get("price", 0)),
                "stop_price": float(o.get("stopPrice", 0)),
                "quantity": float(o.get("origQty", 0)),
                "executed_qty": float(o.get("executedQty", 0)),
                "status": o["status"],
                "time_in_force": o.get("timeInForce", ""),
                "reduce_only": o.get("reduceOnly", False),
                "working_type": o.get("workingType", ""),
                "time": int(o.get("time", 0)),
                "algo_order": False,
            })

        # Algo/conditional orders (STOP_MARKET, TAKE_PROFIT_MARKET, etc.)
        try:
            algo_data = await self.get_open_algo_orders(symbol=symbol)
            algo_orders = algo_data.get("orders", [])
            orders.extend(algo_orders)
        except Exception:
            pass  # Don't fail if algo query errors; standard orders still valid

        return {"count": len(orders), "orders": orders}

    async def cancel_order(self, symbol: str, order_id: int) -> dict:
        """Cancel a specific open order (standard or algo).
        Tries standard cancel first; if it fails, tries algo cancel."""
        symbol = symbol.upper()
        try:
            result = await self._signed_delete("/fapi/v1/order", {
                "symbol": symbol,
                "orderId": order_id,
            })
            return self._format_order_response(result)
        except httpx.HTTPStatusError:
            # Might be an algo order — try cancelling via algo API
            try:
                return await self.cancel_algo_order(order_id)
            except Exception as e2:
                raise e2

    async def cancel_all_orders(self, symbol: str) -> dict:
        """Cancel ALL open orders for a symbol (standard + algo)."""
        symbol = symbol.upper()
        results = {}

        # Cancel standard orders
        try:
            r = await self._signed_delete("/fapi/v1/allOpenOrders", {
                "symbol": symbol,
            })
            results["standard"] = r
        except Exception as e:
            results["standard_error"] = str(e)

        # Cancel algo/conditional orders
        try:
            r = await self.cancel_all_algo_orders(symbol)
            results["algo"] = r
        except Exception as e:
            results["algo_error"] = str(e)

        return {
            "symbol": symbol,
            "result": results,
        }

    # ─────────────────────────────────────────────
    # PERFORMANCE / TRADE HISTORY
    # ─────────────────────────────────────────────

    async def get_income_history(
        self,
        income_type: Optional[str] = None,
        symbol: Optional[str] = None,
        days: int = 30,
        limit: int = 1000,
    ) -> list[dict]:
        """
        Fetch income history (realized PnL, funding, commissions, etc.).
        Uses /fapi/v1/income with pagination to get all records in the period.
        """
        start_time = int((time.time() - days * 86400) * 1000)
        params: dict = {"startTime": start_time, "limit": limit}
        if income_type:
            params["incomeType"] = income_type
        if symbol:
            params["symbol"] = symbol.upper()

        all_records: list[dict] = []
        while True:
            data = await self._signed_get("/fapi/v1/income", dict(params))
            if not data:
                break
            all_records.extend(data)
            if len(data) < limit:
                break
            # Paginate forward
            params["startTime"] = int(data[-1]["time"]) + 1

        return all_records

    async def get_trade_history(
        self,
        symbol: str,
        days: int = 30,
        limit: int = 1000,
    ) -> list[dict]:
        """Fetch user trades for a specific symbol."""
        start_time = int((time.time() - days * 86400) * 1000)
        params = {
            "symbol": symbol.upper(),
            "startTime": start_time,
            "limit": limit,
        }
        data = await self._signed_get("/fapi/v1/userTrades", params)
        return data

    async def get_performance(
        self,
        days: int = 30,
        symbol: Optional[str] = None,
    ) -> dict:
        """
        Compute trading performance summary over a period.

        Returns:
        - Realized PnL breakdown (by symbol, by day)
        - Funding income/cost
        - Commission costs
        - Win/loss stats
        - Net performance
        """
        # Fetch all income records for the period
        income_records = await self.get_income_history(
            symbol=symbol, days=days, limit=1000
        )

        # Also fetch current balance for context
        balance = await self.get_account_balance()

        # Categorize income
        realized_pnl = []
        funding = []
        commissions = []
        transfers = []
        other_income = []

        for r in income_records:
            income = float(r.get("income", 0))
            itype = r.get("incomeType", "")
            sym = r.get("symbol", "")
            ts = int(r.get("time", 0))
            entry = {
                "income": income,
                "symbol": sym,
                "time": ts,
                "type": itype,
                "info": r.get("info", ""),
                "tradeId": r.get("tradeId", ""),
            }

            if itype == "REALIZED_PNL":
                realized_pnl.append(entry)
            elif itype == "FUNDING_FEE":
                funding.append(entry)
            elif itype in ("COMMISSION", "COMMISSION_REBATE"):
                commissions.append(entry)
            elif itype in ("TRANSFER", "INTERNAL_TRANSFER"):
                transfers.append(entry)
            else:
                other_income.append(entry)

        # ── Realized PnL analysis ──
        total_realized = sum(r["income"] for r in realized_pnl)
        wins = [r for r in realized_pnl if r["income"] > 0]
        losses = [r for r in realized_pnl if r["income"] < 0]
        total_wins = sum(r["income"] for r in wins)
        total_losses = sum(r["income"] for r in losses)

        win_count = len(wins)
        loss_count = len(losses)
        total_trades = win_count + loss_count
        win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0

        avg_win = (total_wins / win_count) if win_count > 0 else 0
        avg_loss = (total_losses / loss_count) if loss_count > 0 else 0
        profit_factor = (total_wins / abs(total_losses)) if total_losses != 0 else float("inf") if total_wins > 0 else 0
        expectancy = (total_realized / total_trades) if total_trades > 0 else 0

        # Largest win/loss
        largest_win = max((r["income"] for r in wins), default=0)
        largest_loss = min((r["income"] for r in losses), default=0)

        # By symbol
        pnl_by_symbol: dict[str, dict] = {}
        for r in realized_pnl:
            sym = r["symbol"] or "UNKNOWN"
            if sym not in pnl_by_symbol:
                pnl_by_symbol[sym] = {"pnl": 0.0, "wins": 0, "losses": 0, "trades": 0}
            pnl_by_symbol[sym]["pnl"] += r["income"]
            pnl_by_symbol[sym]["trades"] += 1
            if r["income"] > 0:
                pnl_by_symbol[sym]["wins"] += 1
            elif r["income"] < 0:
                pnl_by_symbol[sym]["losses"] += 1

        # Round and sort by PnL
        symbol_breakdown = []
        for sym, stats in sorted(pnl_by_symbol.items(), key=lambda x: x[1]["pnl"], reverse=True):
            wr = (stats["wins"] / stats["trades"] * 100) if stats["trades"] > 0 else 0
            symbol_breakdown.append({
                "symbol": sym,
                "pnl": round(stats["pnl"], 4),
                "trades": stats["trades"],
                "wins": stats["wins"],
                "losses": stats["losses"],
                "win_rate": round(wr, 1),
            })

        # By day
        daily_pnl: dict[str, float] = {}
        for r in realized_pnl:
            from datetime import datetime, timezone
            day = datetime.fromtimestamp(r["time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            daily_pnl[day] = daily_pnl.get(day, 0) + r["income"]

        daily_breakdown = [
            {"date": d, "pnl": round(v, 4)}
            for d, v in sorted(daily_pnl.items())
        ]

        # ── Funding analysis ──
        total_funding = sum(r["income"] for r in funding)
        # Negative funding income = you paid (you were on the dominant side)
        # Positive funding income = you received
        funding_by_symbol: dict[str, float] = {}
        for r in funding:
            sym = r["symbol"] or "UNKNOWN"
            funding_by_symbol[sym] = funding_by_symbol.get(sym, 0) + r["income"]

        funding_breakdown = [
            {"symbol": s, "funding": round(v, 4)}
            for s, v in sorted(funding_by_symbol.items(), key=lambda x: x[1])
        ]

        # ── Commissions ──
        total_commissions = sum(r["income"] for r in commissions)

        # ── Net performance ──
        net_pnl = total_realized + total_funding + total_commissions

        # Winning/losing days
        winning_days = sum(1 for d in daily_pnl.values() if d > 0)
        losing_days = sum(1 for d in daily_pnl.values() if d < 0)
        best_day = max(daily_pnl.values(), default=0)
        worst_day = min(daily_pnl.values(), default=0)

        # Max drawdown from daily PnL series
        cumulative = []
        running = 0
        peak = 0
        max_dd = 0
        for d in sorted(daily_pnl.keys()):
            running += daily_pnl[d]
            cumulative.append(running)
            peak = max(peak, running)
            dd = peak - running
            max_dd = max(max_dd, dd)

        # ROI estimate (if balance > 0)
        usdt_balance = balance.get("usdt_balance", 0)
        estimated_capital = usdt_balance - net_pnl if usdt_balance > 0 else 0
        roi_pct = (net_pnl / estimated_capital * 100) if estimated_capital > 0 else 0

        return {
            "period_days": days,
            "filter_symbol": symbol,
            "summary": {
                "net_pnl": round(net_pnl, 4),
                "realized_pnl": round(total_realized, 4),
                "funding_income": round(total_funding, 4),
                "commissions": round(total_commissions, 4),
                "roi_pct": round(roi_pct, 2),
                "estimated_starting_capital": round(estimated_capital, 2),
                "current_balance": round(usdt_balance, 2),
            },
            "trade_stats": {
                "total_trades": total_trades,
                "wins": win_count,
                "losses": loss_count,
                "win_rate": round(win_rate, 1),
                "avg_win": round(avg_win, 4),
                "avg_loss": round(avg_loss, 4),
                "largest_win": round(largest_win, 4),
                "largest_loss": round(largest_loss, 4),
                "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else "inf",
                "expectancy": round(expectancy, 4),
            },
            "risk_metrics": {
                "max_drawdown": round(max_dd, 4),
                "winning_days": winning_days,
                "losing_days": losing_days,
                "best_day": round(best_day, 4),
                "worst_day": round(worst_day, 4),
            },
            "by_symbol": symbol_breakdown,
            "by_day": daily_breakdown,
            "funding_breakdown": funding_breakdown,
            "total_income_records": len(income_records),
        }

    # ─────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────

    def _format_order_response(
        self,
        raw: dict,
        leverage_info: Optional[dict] = None,
        closing: bool = False,
        original_position: Optional[dict] = None,
    ) -> dict:
        """Format raw Binance order response into clean dict."""
        avg_price = float(raw.get("avgPrice", 0))
        if avg_price == 0:
            avg_price = float(raw.get("price", 0))

        executed_qty = float(raw.get("executedQty", 0))
        cum_quote = float(raw.get("cumQuote", 0))

        result = {
            "order_id": raw.get("orderId"),
            "symbol": raw.get("symbol"),
            "side": raw.get("side"),
            "type": raw.get("type"),
            "status": raw.get("status"),
            "price": float(raw.get("price", 0)),
            "avg_price": avg_price,
            "stop_price": float(raw.get("stopPrice", 0)),
            "quantity": float(raw.get("origQty", 0)),
            "executed_qty": executed_qty,
            "cum_quote_usdt": cum_quote,
            "reduce_only": raw.get("reduceOnly", False),
            "time": int(raw.get("updateTime", 0)),
        }

        if leverage_info:
            result["leverage_set"] = leverage_info

        if closing and original_position:
            result["closed_position"] = {
                "was_side": original_position["side"],
                "entry_price": original_position["entry_price"],
                "pnl_at_close": original_position.get("unrealized_pnl"),
            }

        return result
