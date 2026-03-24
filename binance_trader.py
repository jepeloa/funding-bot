"""
Binance Futures Trading Client para αf Bifurcation Short.

Cliente autenticado para operar en Binance USDT-M Futures.
Soporta múltiples cuentas (principal, copytrading) configurables en config.py.

Operaciones:
  - Abrir posición SHORT (MARKET)
  - Cerrar posición (MARKET, reduce-only)
  - Colocar TP / SL / trailing stop (STOP_MARKET, TAKE_PROFIT_MARKET)
  - Consultar balance de cuenta (USDT)
  - Consultar posiciones abiertas con PnL
  - Setear leverage por symbol
  - Cancelar órdenes abiertas

Seguridad:
  - HMAC-SHA256 signing en cada request
  - API Key / Secret desde config BINANCE_ACCOUNTS

Referencia: mcp-binance-claude/src/psi_jam_mcp/futures_trader.py
"""

import asyncio
import hashlib
import hmac
import logging
import time
import urllib.parse
from typing import Optional

import httpx

log = logging.getLogger("binance_trader")

FUTURES_BASE_URL = "https://fapi.binance.com"
DEFAULT_RECV_WINDOW = 5000
MAX_RETRIES = 3
RETRY_DELAY = 1.0


class AlgoOrderRequired(Exception):
    """Binance -4120: conditional orders must use /fapi/v1/algoOrder."""
    pass


class BinanceTrader:
    """
    Cliente autenticado para Binance USDT-M Futures.

    Diseñado para ser instanciado por cuenta:
      trader = BinanceTrader(api_key, api_secret, account_name="principal")
    """

    def __init__(self, api_key: str, api_secret: str,
                 account_name: str = "default",
                 recv_window: int = DEFAULT_RECV_WINDOW):
        self.api_key = api_key
        self.api_secret = api_secret
        self.account_name = account_name
        self.recv_window = recv_window
        self._client: Optional[httpx.AsyncClient] = None
        self._symbol_info_cache: dict = {}
        self._hedge_mode: Optional[bool] = None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  LIFECYCLE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def connect(self):
        """Crear httpx client y detectar hedge mode."""
        self._client = httpx.AsyncClient(
            base_url=FUTURES_BASE_URL,
            timeout=30.0,
            headers={
                "X-MBX-APIKEY": self.api_key,
                "User-Agent": "AlphaF-Strategy/2.0",
            },
        )
        self._hedge_mode = await self._detect_hedge_mode()
        log.info(
            f"[{self.account_name}] Conectado a Binance Futures — "
            f"hedge_mode={self._hedge_mode}"
        )

    async def close(self):
        """Cerrar httpx client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SIGNING (HMAC-SHA256)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _sign(self, params: dict) -> dict:
        """Añadir timestamp y firma HMAC-SHA256."""
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = self.recv_window
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        params["signature"] = signature
        return params

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  HTTP HELPERS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _signed_get(self, path: str, params: dict = None) -> dict:
        params = self._sign(params or {})
        resp = await self._client.get(path, params=params)
        data = resp.json()
        if resp.status_code != 200:
            raise Exception(f"Binance GET {path} error: {data}")
        return data

    async def _signed_post(self, path: str, params: dict = None,
                           retries: int = MAX_RETRIES) -> dict:
        params = self._sign(params or {})
        for attempt in range(retries):
            resp = await self._client.post(path, params=params)
            data = resp.json()
            if resp.status_code == 200:
                return data
            code = data.get("code", 0)
            # -4120: AlgoOrderRequired (conditional orders since Dec 2025)
            if code == -4120:
                raise AlgoOrderRequired(data.get("msg", ""))
            # -1008: Server overloaded, retry
            if code == -1008 and attempt < retries - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                params = self._sign({k: v for k, v in params.items()
                                     if k not in ("timestamp", "recvWindow", "signature")})
                continue
            raise Exception(f"Binance POST {path} error: {data}")
        raise Exception(f"Binance POST {path}: max retries exceeded")

    async def _signed_delete(self, path: str, params: dict = None) -> dict:
        params = self._sign(params or {})
        resp = await self._client.delete(path, params=params)
        data = resp.json()
        if resp.status_code != 200:
            raise Exception(f"Binance DELETE {path} error: {data}")
        return data

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  ACCOUNT
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def get_account_balance(self) -> dict:
        """Obtener balance USDT: total, available, unrealized PnL."""
        balances = await self._signed_get("/fapi/v2/balance")
        for b in balances:
            if b["asset"] == "USDT":
                return {
                    "asset": "USDT",
                    "balance": float(b["balance"]),
                    "available": float(b["availableBalance"]),
                    "cross_wallet": float(b.get("crossWalletBalance", 0)),
                    "unrealized_pnl": float(b.get("crossUnPnl", 0)),
                }
        return {"asset": "USDT", "balance": 0, "available": 0,
                "cross_wallet": 0, "unrealized_pnl": 0}

    async def get_positions(self, symbol: str = None) -> list[dict]:
        """Obtener posiciones abiertas con PnL, ROE, leverage."""
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        raw = await self._signed_get("/fapi/v2/positionRisk", params)
        positions = []
        for p in raw:
            amt = float(p.get("positionAmt", 0))
            if amt == 0:
                continue
            entry = float(p.get("entryPrice", 0))
            mark = float(p.get("markPrice", 0))
            pnl = float(p.get("unRealizedProfit", 0))
            leverage = int(p.get("leverage", 1))
            notional = abs(amt) * mark
            margin = notional / leverage if leverage else notional
            roe = (pnl / margin * 100) if margin > 0 else 0
            liq = float(p.get("liquidationPrice", 0))

            positions.append({
                "symbol": p["symbol"],
                "side": "SHORT" if amt < 0 else "LONG",
                "position_amt": abs(amt),
                "entry_price": entry,
                "mark_price": mark,
                "unrealized_pnl": pnl,
                "leverage": leverage,
                "notional": notional,
                "margin": margin,
                "roe_pct": roe,
                "liquidation_price": liq,
                "position_side": p.get("positionSide", "BOTH"),
            })
        return positions

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  HEDGE MODE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _detect_hedge_mode(self) -> bool:
        """Detectar si la cuenta usa hedge mode (dual position side)."""
        try:
            data = await self._signed_get("/fapi/v1/positionSide/dual")
            return data.get("dualSidePosition", False)
        except Exception:
            return False

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  SYMBOL INFO & PRECISION
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _get_symbol_info(self, symbol: str) -> dict:
        """Obtener precision info para un symbol (cacheado)."""
        sym = symbol.upper()
        if sym in self._symbol_info_cache:
            return self._symbol_info_cache[sym]
        resp = await self._client.get("/fapi/v1/exchangeInfo",
                                       params={"symbol": sym})
        data = resp.json()
        for s in data.get("symbols", []):
            if s["symbol"] == sym:
                info = {
                    "symbol": sym,
                    "price_precision": s["pricePrecision"],
                    "qty_precision": s["quantityPrecision"],
                    "filters": {f["filterType"]: f for f in s["filters"]},
                }
                self._symbol_info_cache[sym] = info
                return info
        raise Exception(f"Symbol {sym} not found in exchangeInfo")

    def _round_price(self, price: float, info: dict) -> float:
        return round(price, info["price_precision"])

    def _round_qty(self, qty: float, info: dict) -> float:
        return round(qty, info["qty_precision"])

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  LEVERAGE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def set_leverage(self, symbol: str, leverage: int) -> dict:
        """Setear leverage para un symbol (1-125x)."""
        return await self._signed_post("/fapi/v1/leverage", {
            "symbol": symbol.upper(),
            "leverage": leverage,
        })

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  OPEN POSITION (SHORT MARKET)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def open_short(self, symbol: str, quantity: float,
                         leverage: int = None,
                         take_profit: float = None,
                         stop_loss: float = None) -> dict:
        """
        Abrir posición SHORT via MARKET.

        Args:
            symbol: ej "BTCUSDT"
            quantity: cantidad en base asset
            leverage: si se pasa, setea antes de abrir
            take_profit: precio de TP (opcional)
            stop_loss: precio de SL (opcional)

        Returns:
            dict con orderId, avgPrice, executedQty, etc.
        """
        sym = symbol.upper()
        info = await self._get_symbol_info(sym)
        qty = self._round_qty(quantity, info)

        # Setear leverage si se especifica
        if leverage:
            await self.set_leverage(sym, leverage)

        # Orden MARKET SHORT (FULL para obtener fills con commission)
        params = {
            "symbol": sym,
            "side": "SELL",
            "type": "MARKET",
            "quantity": str(qty),
            "newOrderRespType": "FULL",
        }
        # Hedge mode → positionSide
        if self._hedge_mode:
            params["positionSide"] = "SHORT"

        log.info(f"[{self.account_name}] OPEN SHORT {sym} qty={qty} lev={leverage}")
        result = await self._signed_post("/fapi/v1/order", params)

        # Extraer commission real de los fills
        result["totalCommission"] = sum(
            float(f.get("commission", 0)) for f in result.get("fills", [])
        )

        # Colocar TP/SL tras fill confirmado
        fill_status = result.get("status", "")
        filled_qty = float(result.get("executedQty", 0))
        if (take_profit or stop_loss) and fill_status == "FILLED" and filled_qty > 0:
            await self.set_tp_sl(sym, filled_qty, take_profit, stop_loss)

        return result

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  CLOSE POSITION
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def close_position(self, symbol: str,
                             quantity: float = None) -> dict:
        """
        Cerrar posición existente (total o parcial).

        Si quantity=None, cierra la posición completa.
        Detecta el side automáticamente.
        """
        sym = symbol.upper()
        positions = await self.get_positions(sym)
        if not positions:
            raise Exception(f"No open position for {sym}")

        pos = positions[0]
        close_qty = quantity or pos["position_amt"]
        info = await self._get_symbol_info(sym)
        close_qty = self._round_qty(close_qty, info)

        # Close side = opuesto al position side
        close_side = "BUY" if pos["side"] == "SHORT" else "SELL"

        params = {
            "symbol": sym,
            "side": close_side,
            "type": "MARKET",
            "quantity": str(close_qty),
            "newOrderRespType": "FULL",
        }
        if self._hedge_mode:
            params["positionSide"] = pos["position_side"]
        else:
            params["reduceOnly"] = "true"

        log.info(
            f"[{self.account_name}] CLOSE {pos['side']} {sym} "
            f"qty={close_qty}"
        )
        result = await self._signed_post("/fapi/v1/order", params)

        # Extraer commission real de los fills
        result["totalCommission"] = sum(
            float(f.get("commission", 0)) for f in result.get("fills", [])
        )

        return result

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  TP / SL (CONDITIONAL ORDERS)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def set_tp_sl(self, symbol: str, quantity: float,
                        take_profit: float = None,
                        stop_loss: float = None) -> list[dict]:
        """
        Colocar TP y/o SL para una posición SHORT existente.

        Usa MARK_PRICE como workingType para evitar wicks en el last price.
        Si Binance retorna -4120 (AlgoOrderRequired), usa /fapi/v1/algoOrder.
        """
        sym = symbol.upper()
        info = await self._get_symbol_info(sym)
        qty_str = str(self._round_qty(quantity, info))
        results = []

        tasks = []
        if take_profit:
            tp_price = self._round_price(take_profit, info)
            tasks.append(self._place_conditional_order(
                sym, "BUY", "TAKE_PROFIT_MARKET", tp_price, qty_str, info
            ))

        if stop_loss:
            sl_price = self._round_price(stop_loss, info)
            tasks.append(self._place_conditional_order(
                sym, "BUY", "STOP_MARKET", sl_price, qty_str, info
            ))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    log.error(f"[{self.account_name}] TP/SL error: {r}")
                    results[i] = {"error": str(r)}

        return results

    async def _place_conditional_order(self, symbol: str, side: str,
                                        order_type: str, stop_price: float,
                                        quantity: str, info: dict) -> dict:
        """Colocar orden condicional (TP o SL). Fallback a Algo API en -4120."""
        params = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "stopPrice": str(self._round_price(stop_price, info)),
            "quantity": quantity,
            "workingType": "MARK_PRICE",
            "priceProtect": "true",
        }
        if self._hedge_mode:
            params["positionSide"] = "SHORT"
        else:
            params["reduceOnly"] = "true"

        try:
            return await self._signed_post("/fapi/v1/order", params)
        except AlgoOrderRequired:
            log.info(f"[{self.account_name}] Using algo order API for {order_type}")
            return await self._place_algo_order(params)

    async def _place_algo_order(self, params: dict) -> dict:
        """Colocar orden via /fapi/v1/algoOrder (API condicional nueva)."""
        algo_params = {
            "symbol": params["symbol"],
            "side": params["side"],
            "positionSide": params.get("positionSide", "BOTH"),
            "quantity": params["quantity"],
            "algoType": "CONDITIONAL",
            "triggerPrice": params["stopPrice"],
            "workingType": params.get("workingType", "MARK_PRICE"),
        }
        # Map order type
        ot = params.get("type", "")
        if "TAKE_PROFIT" in ot:
            algo_params["side"] = params["side"]
        return await self._signed_post("/fapi/v1/algoOrder", algo_params)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  ORDER MANAGEMENT
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def get_open_orders(self, symbol: str = None) -> list[dict]:
        """Obtener órdenes abiertas (standard + algo)."""
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()

        # Standard orders
        std = await self._signed_get("/fapi/v1/openOrders", params)

        # Algo orders
        try:
            algo_params = {}
            if symbol:
                algo_params["symbol"] = symbol.upper()
            algo = await self._signed_get("/fapi/v1/openAlgoOrders", algo_params)
            algo_list = algo.get("orders", []) if isinstance(algo, dict) else []
        except Exception:
            algo_list = []

        return std + algo_list

    async def cancel_all_orders(self, symbol: str) -> dict:
        """Cancelar todas las órdenes abiertas para un symbol."""
        sym = symbol.upper()
        results = {}

        # Standard orders
        try:
            r = await self._signed_delete("/fapi/v1/allOpenOrders",
                                           {"symbol": sym})
            results["standard"] = r
        except Exception as e:
            results["standard_error"] = str(e)

        # Algo orders
        try:
            r = await self._signed_delete("/fapi/v1/algoOpenOrders",
                                           {"symbol": sym})
            results["algo"] = r
        except Exception as e:
            results["algo_error"] = str(e)

        return results

    async def cancel_order(self, symbol: str, order_id: int) -> dict:
        """Cancelar una orden específica."""
        return await self._signed_delete("/fapi/v1/order", {
            "symbol": symbol.upper(),
            "orderId": order_id,
        })

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  INCOME / HISTORY
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def get_income_history(self, days: int = 7,
                                  income_type: str = None,
                                  symbol: str = None,
                                  limit: int = 1000) -> list[dict]:
        """Historial de income (PnL, funding, commission, etc.)."""
        params = {
            "startTime": int((time.time() - days * 86400) * 1000),
            "limit": min(limit, 1000),
        }
        if income_type:
            params["incomeType"] = income_type
        if symbol:
            params["symbol"] = symbol.upper()
        return await self._signed_get("/fapi/v1/income", params)

    async def get_trade_history(self, symbol: str,
                                 days: int = 7,
                                 limit: int = 500) -> list[dict]:
        """Historial de trades para un symbol."""
        params = {
            "symbol": symbol.upper(),
            "startTime": int((time.time() - days * 86400) * 1000),
            "limit": min(limit, 1000),
        }
        return await self._signed_get("/fapi/v1/userTrades", params)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  CONVENIENCE: calcular la cantidad para un nocional dado
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def calc_quantity(self, symbol: str, notional: float,
                            price: float) -> float:
        """
        Calcular qty base dado un nocional USDT y precio.
        qty = notional / price, rounded to symbol precision.
        """
        info = await self._get_symbol_info(symbol.upper())
        raw_qty = notional / price
        return self._round_qty(raw_qty, info)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  HEALTH CHECK
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def ping(self) -> bool:
        """Test connectivity and authentication."""
        try:
            bal = await self.get_account_balance()
            return bal.get("balance", 0) >= 0
        except Exception as e:
            log.error(f"[{self.account_name}] Ping failed: {e}")
            return False

    def __repr__(self):
        masked = self.api_key[:8] + "..." + self.api_key[-4:]
        return f"BinanceTrader(account={self.account_name}, key={masked})"
