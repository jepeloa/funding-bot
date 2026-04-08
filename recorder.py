"""
Recorder de Binance Futures USDT-M — TODOS los pares.

Arquitectura:
  - Descubre automáticamente todos los pares USDT-M activos via REST
  - Abre múltiples WebSockets en paralelo (≤200 streams cada uno)
  - Graba: depth, aggTrade, bookTicker, markPrice (WS) + OI, funding (REST poll)
  - Strategy engine evalúa señales y genera paper trades en la DB
  - Robustez: reconexión automática, crash recovery, heartbeat, flush de emergencia
"""

import asyncio
import json
import logging
import logging.handlers
import math
import os
import signal
import sys
import time
import traceback

import aiohttp
import websockets

from config import (
    SYMBOLS,
    BINANCE_FUTURES_WS_BASE,
    BINANCE_FUTURES_REST,
    DEPTH_LEVELS,
    DEPTH_SPEED,
    RECONNECT_DELAY_SECS,
    MAX_RECONNECT_ATTEMPTS,
    MAX_STREAMS_PER_WS,
    LOG_LEVEL,
    OI_POLL_INTERVAL_SECS,
    OI_PAUSE_BETWEEN_SYMBOLS,
    FUNDING_POLL_INTERVAL_SECS,
    FUNDING_INFO_POLL_INTERVAL_SECS,
    LSR_POLL_INTERVAL_SECS,
    TAKER_VOL_POLL_INTERVAL_SECS,
    SENTIMENT_PAUSE_BETWEEN,
    SENTIMENT_TOP_N,
    DATA_DIR,
)
from db import init_db, AsyncDBWriter
from strategy import StrategyEngine

# ── Logging ────────────────────────────────────────────────────────
_LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

_LOG_FORMAT = "%(asctime)s │ %(levelname)-7s │ %(name)-10s │ %(message)s"
_LOG_DATE   = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=_LOG_FORMAT,
    datefmt=_LOG_DATE,
)

# Archivo rotativo: 50 MB × 10 archivos ≈ 500 MB total
_file_handler = logging.handlers.RotatingFileHandler(
    filename=os.path.join(_LOG_DIR, "recorder.log"),
    maxBytes=50 * 1024 * 1024,
    backupCount=10,
    encoding="utf-8",
)
_file_handler.setLevel(getattr(logging, LOG_LEVEL))
_file_handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATE))
logging.getLogger().addHandler(_file_handler)

log = logging.getLogger("recorder")

# ── PID file ───────────────────────────────────────────────────────
PID_FILE = os.path.join(os.path.dirname(__file__), "recorder.pid")

# ── Señal de parada ────────────────────────────────────────────────
shutdown_event = asyncio.Event()

# ── Tracking de actividad WS (para watchdog) ──────────────────────
_ws_last_msg_time: float = 0.0       # epoch del último msg WS recibido
_ws_tasks: list[asyncio.Task] = []   # refs a tareas WS para cancelarlas


# ══════════════════════════════════════════════════════════════════
#  DESCUBRIMIENTO DE SÍMBOLOS
# ══════════════════════════════════════════════════════════════════

async def fetch_all_futures_symbols() -> list[str]:
    """Descubre TODOS los pares USDT-M activos en Binance Futures.
    Incluye PERPETUAL + TRADIFI_PERPETUAL (XAU, TSLA, etc.)."""
    url = f"{BINANCE_FUTURES_REST}/fapi/v1/exchangeInfo"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            data = await resp.json()

    symbols = []
    for s in data.get("symbols", []):
        if (s.get("contractType") in ("PERPETUAL", "TRADIFI_PERPETUAL")
                and s.get("status") == "TRADING"
                and s.get("quoteAsset") == "USDT"):
            symbols.append(s["symbol"].lower())

    symbols.sort()
    return symbols


# ══════════════════════════════════════════════════════════════════
#  MULTI-WS MANAGER
# ══════════════════════════════════════════════════════════════════

def build_stream_groups(symbols: list[str]) -> list[list[str]]:
    """
    Construye grupos de streams respetando el límite de 200/WS.
    Streams por símbolo: depth, aggTrade, bookTicker, markPrice = 4
    """
    streams_per_symbol = 4  # depth, aggTrade, bookTicker, markPrice
    symbols_per_ws = MAX_STREAMS_PER_WS // streams_per_symbol

    groups = []
    for i in range(0, len(symbols), symbols_per_ws):
        chunk = symbols[i:i + symbols_per_ws]
        streams = []
        for s in chunk:
            streams.append(f"{s}@depth{DEPTH_LEVELS}@{DEPTH_SPEED}")
            streams.append(f"{s}@aggTrade")
            streams.append(f"{s}@bookTicker")
            streams.append(f"{s}@markPrice@1s")
        groups.append(streams)
    return groups


async def ws_listener(group_id: int, streams: list[str],
                      writer: AsyncDBWriter, strategy: StrategyEngine):
    """
    Un listener WS para un grupo de streams.
    Reconexión automática con backoff exponencial.
    """
    combined = "/".join(streams)
    url = f"{BINANCE_FUTURES_WS_BASE}/stream?streams={combined}"
    attempt = 0

    n_symbols = len(streams) // 4
    log.info(f"[WS-{group_id}] {n_symbols} símbolos, {len(streams)} streams")

    while not shutdown_event.is_set():
        connected_at = 0.0
        try:
            attempt += 1

            async with websockets.connect(
                url,
                ping_interval=20,   # Cliente envía ping cada 20s
                ping_timeout=10,    # Cierra si no hay pong en 10s
                max_size=2**22,     # 4MB
                close_timeout=5,
            ) as ws:
                log.info(f"[WS-{group_id}] Conectado (intento {attempt})")
                connected_at = time.time()

                while not shutdown_event.is_set():
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(), timeout=60.0
                        )
                    except asyncio.TimeoutError:
                        log.warning(
                            f"[WS-{group_id}] Sin datos en 60s, "
                            f"forzando reconexión"
                        )
                        break
                    try:
                        msg = json.loads(raw)
                        await handle_message(msg, writer, strategy)
                    except Exception as e:
                        log.error(
                            f"[WS-{group_id}] Error procesando msg: {e}"
                        )

        except asyncio.CancelledError:
            break
        except Exception as e:
            if shutdown_event.is_set():
                break
            log.warning(f"[WS-{group_id}] Desconectado: {type(e).__name__}: {e}")

        if shutdown_event.is_set():
            break

        # Solo resetear backoff si la conexión fue estable (>60s)
        if connected_at and (time.time() - connected_at) > 60:
            attempt = 0

        if MAX_RECONNECT_ATTEMPTS and attempt >= MAX_RECONNECT_ATTEMPTS:
            log.error(f"[WS-{group_id}] Max reintentos. Saliendo.")
            break

        backoff = min(RECONNECT_DELAY_SECS * (2 ** min(attempt - 1, 6)), 120)
        log.info(f"[WS-{group_id}] Reconectando en {backoff:.0f}s...")
        await asyncio.sleep(backoff)


# ══════════════════════════════════════════════════════════════════
#  MESSAGE HANDLER
# ══════════════════════════════════════════════════════════════════

async def handle_message(msg: dict, writer: AsyncDBWriter, strategy: StrategyEngine):
    """Routea cada mensaje al insert correcto y al strategy engine."""
    global _ws_last_msg_time
    received_at = time.time()
    _ws_last_msg_time = received_at
    stream: str = msg.get("stream", "")
    data: dict = msg.get("data", {})

    if not stream or not data:
        return

    symbol = stream.split("@")[0].upper()

    if "@depth" in stream:
        await writer.insert_depth(symbol, data, received_at)

    elif "@aggTrade" in stream:
        await writer.insert_trade(symbol, data, received_at)
        # Feed strategy con cada trade
        state = strategy.get_state(symbol)
        state.update_trade(
            price=float(data["p"]),
            qty=float(data["q"]),
            is_buyer_maker=data["m"],
            trade_time_ms=data["T"],
        )

    elif "@bookTicker" in stream:
        await writer.insert_ticker(symbol, data, received_at)

    elif "@markPrice" in stream:
        await writer.insert_mark_price(symbol, data, received_at)
        # Feed strategy con mark price
        state = strategy.get_state(symbol)
        state.update_mark_price(
            mark=float(data["p"]),
            index=float(data["i"]),
            funding=float(data.get("r", "0")),
            next_fund_ts=int(data.get("T", 0)),
            ts=received_at,
        )
        # MFE/MAE tracking en tiempo real (cada @markPrice tick)
        strategy.update_trade_mfe(symbol, float(data["p"]))

    elif "forceOrder" in stream:
        # Liquidation — symbol viene en el payload, no en stream name
        liq_symbol = data.get("o", {}).get("s", symbol)
        await writer.insert_liquidation(liq_symbol, data, received_at)


# ══════════════════════════════════════════════════════════════════
#  REST POLLERS (OI + Funding)
# ══════════════════════════════════════════════════════════════════

async def poll_open_interest(symbols: list[str], writer: AsyncDBWriter,
                             strategy: StrategyEngine):
    """Pollea OI para todos los símbolos periódicamente via REST."""
    log.info(f"OI poller iniciado: {len(symbols)} símbolos cada {OI_POLL_INTERVAL_SECS}s")

    while not shutdown_event.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                for sym in symbols:
                    if shutdown_event.is_set():
                        break
                    try:
                        url = f"{BINANCE_FUTURES_REST}/fapi/v1/openInterest"
                        async with session.get(
                            url,
                            params={"symbol": sym.upper()},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                now = time.time()
                                oi_qty = data.get("openInterest", "0")
                                # Calcular valor en USDT
                                state = strategy.get_state(sym.upper())
                                price = state.mark_price if state.mark_price > 0 else 1.0
                                oi_value = str(float(oi_qty) * price)

                                await writer.insert_oi(sym.upper(), oi_qty, oi_value, now)
                                state.update_oi(float(oi_qty), float(oi_value), now)
                            elif resp.status == 429:
                                log.warning("OI poller: rate limited, pausing 60s")
                                await asyncio.sleep(60)
                                break
                    except asyncio.CancelledError:
                        return
                    except Exception as e:
                        log.debug(f"OI error {sym}: {e}")

                    await asyncio.sleep(OI_PAUSE_BETWEEN_SYMBOLS)

        except asyncio.CancelledError:
            return
        except Exception as e:
            log.error(f"OI poller error: {e}")

        # Esperar hasta el próximo ciclo
        for _ in range(int(OI_POLL_INTERVAL_SECS)):
            if shutdown_event.is_set():
                return
            await asyncio.sleep(1)


async def poll_funding_rates(symbols: list[str], writer: AsyncDBWriter):
    """Pollea funding rates periódicamente."""
    log.info(f"Funding poller iniciado cada {FUNDING_POLL_INTERVAL_SECS}s")

    while not shutdown_event.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{BINANCE_FUTURES_REST}/fapi/v1/premiumIndex"
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 200:
                        now = time.time()
                        data = await resp.json()
                        for item in data:
                            sym = item.get("symbol", "")
                            rate = item.get("lastFundingRate", "0")
                            fund_time = item.get("nextFundingTime", 0)
                            if sym and rate:
                                await writer.insert_funding(sym, fund_time, rate, now)
                    elif resp.status == 429:
                        log.warning("Funding poller: rate limited")
                        await asyncio.sleep(60)
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.error(f"Funding poller error: {e}")

        for _ in range(int(FUNDING_POLL_INTERVAL_SECS)):
            if shutdown_event.is_set():
                return
            await asyncio.sleep(1)


async def poll_funding_info(strategy: StrategyEngine):
    """
    Consulta /fapi/v1/fundingInfo para obtener el intervalo dinámico de funding
    por activo (1h/2h/4h/8h). Actualiza state.funding_interval_secs.
    Binance cambia el intervalo cuando el rate toca el cap (FOMO extremo).
    """
    log.info(f"FundingInfo poller iniciado cada {FUNDING_INFO_POLL_INTERVAL_SECS}s")

    while not shutdown_event.is_set():
        changed = 0
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{BINANCE_FUTURES_REST}/fapi/v1/fundingInfo"
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for item in data:
                            sym = item.get("symbol", "")
                            interval_h = item.get("fundingIntervalHours", 8)
                            if sym:
                                state = strategy.get_state(sym)
                                new_secs = interval_h * 3600
                                if state.funding_interval_secs != new_secs:
                                    log.info(
                                        f"🔔 {sym} funding interval: "
                                        f"{state.funding_interval_secs/3600:.0f}h → {interval_h}h"
                                    )
                                    state.funding_interval_secs = new_secs
                                    changed += 1
                        log.info(
                            f"FundingInfo: {len(data)} símbolos consultados"
                            + (f", {changed} cambiaron intervalo" if changed else "")
                        )
                    elif resp.status == 429:
                        log.warning("FundingInfo: rate limited, retry en 60s")
                        await asyncio.sleep(60)
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.error(f"FundingInfo poller error: {e}")

        for _ in range(int(FUNDING_INFO_POLL_INTERVAL_SECS)):
            if shutdown_event.is_set():
                return
            await asyncio.sleep(1)


# ══════════════════════════════════════════════════════════════════
#  FORCE ORDER (LIQUIDATIONS) — WS DEDICADO
# ══════════════════════════════════════════════════════════════════

async def ws_force_order_listener(writer: AsyncDBWriter):
    """
    Listener WS dedicado para !forceOrder@arr (liquidaciones de todo el mercado).
    Es un stream único que cubre todas las monedas.
    """
    url = f"{BINANCE_FUTURES_WS_BASE}/ws/!forceOrder@arr"
    attempt = 0
    log.info("[WS-liq] Iniciando listener de liquidaciones (!forceOrder@arr)")

    while not shutdown_event.is_set():
        connected_at = 0.0
        try:
            attempt += 1

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=10,
                max_size=2**22,
                close_timeout=5,
            ) as ws:
                log.info(f"[WS-liq] Conectado (intento {attempt})")
                connected_at = time.time()

                while not shutdown_event.is_set():
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(), timeout=3600.0
                        )
                    except asyncio.TimeoutError:
                        log.warning(
                            "[WS-liq] Sin datos en 1h, reconectando"
                        )
                        break
                    try:
                        msg = json.loads(raw)
                        received_at = time.time()
                        data = msg if "o" in msg else msg.get("data", msg)
                        sym = data.get("o", {}).get("s", "")
                        if sym:
                            await writer.insert_liquidation(sym, data, received_at)
                    except Exception as e:
                        log.error(f"[WS-liq] Error procesando: {e}")

        except asyncio.CancelledError:
            break
        except Exception as e:
            if shutdown_event.is_set():
                break
            log.warning(f"[WS-liq] Desconectado: {type(e).__name__}: {e}")

        if shutdown_event.is_set():
            break

        # Solo resetear backoff si la conexión fue estable (>60s)
        if connected_at and (time.time() - connected_at) > 60:
            attempt = 0

        backoff = min(RECONNECT_DELAY_SECS * (2 ** min(attempt - 1, 6)), 120)
        log.info(f"[WS-liq] Reconectando en {backoff:.0f}s...")
        await asyncio.sleep(backoff)


# ══════════════════════════════════════════════════════════════════
#  SENTIMENT POLLERS (L/S Ratio + Taker Volume)
# ══════════════════════════════════════════════════════════════════

async def fetch_top_symbols_by_volume(n: int = 50) -> list[str]:
    """Obtiene los top N símbolos por volumen 24h para sentiment polling."""
    url = f"{BINANCE_FUTURES_REST}/fapi/v1/ticker/24hr"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            data = await resp.json()

    # Filtrar solo USDT perpetuos y ordenar por quoteVolume
    tickers = []
    for t in data:
        sym = t.get("symbol", "")
        vol = float(t.get("quoteVolume", 0))
        if sym.endswith("USDT") and vol > 0:
            tickers.append((sym, vol))

    tickers.sort(key=lambda x: x[1], reverse=True)
    top = [s for s, _ in tickers[:n]]
    return top


async def poll_long_short_ratio(top_symbols: list[str], writer: AsyncDBWriter):
    """Pollea long/short ratio (3 tipos) para top símbolos."""
    log.info(
        f"L/S Ratio poller iniciado: {len(top_symbols)} símbolos "
        f"cada {LSR_POLL_INTERVAL_SECS}s"
    )

    endpoints = [
        ("top_account", "/futures/data/topLongShortAccountRatio"),
        ("top_position", "/futures/data/topLongShortPositionRatio"),
        ("global", "/futures/data/globalLongShortAccountRatio"),
    ]

    while not shutdown_event.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                for sym in top_symbols:
                    if shutdown_event.is_set():
                        break
                    for ratio_type, path in endpoints:
                        try:
                            url = f"{BINANCE_FUTURES_REST}{path}"
                            async with session.get(
                                url,
                                params={"symbol": sym, "period": "5m", "limit": 1},
                                timeout=aiohttp.ClientTimeout(total=10),
                            ) as resp:
                                if resp.status == 200:
                                    now = time.time()
                                    data = await resp.json()
                                    if data and len(data) > 0:
                                        await writer.insert_long_short_ratio(
                                            sym, ratio_type, data[0], now
                                        )
                                elif resp.status == 429:
                                    log.warning("LSR poller: rate limited, pausing 60s")
                                    await asyncio.sleep(60)
                                    break
                        except asyncio.CancelledError:
                            return
                        except Exception as e:
                            log.debug(f"LSR error {sym}/{ratio_type}: {e}")

                        await asyncio.sleep(SENTIMENT_PAUSE_BETWEEN)

        except asyncio.CancelledError:
            return
        except Exception as e:
            log.error(f"LSR poller error: {e}")

        for _ in range(int(LSR_POLL_INTERVAL_SECS)):
            if shutdown_event.is_set():
                return
            await asyncio.sleep(1)


async def poll_taker_buy_sell(top_symbols: list[str], writer: AsyncDBWriter):
    """Pollea taker buy/sell volume para top símbolos."""
    log.info(
        f"Taker volume poller iniciado: {len(top_symbols)} símbolos "
        f"cada {TAKER_VOL_POLL_INTERVAL_SECS}s"
    )

    while not shutdown_event.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                for sym in top_symbols:
                    if shutdown_event.is_set():
                        break
                    try:
                        url = f"{BINANCE_FUTURES_REST}/futures/data/takerlongshortRatio"
                        async with session.get(
                            url,
                            params={"symbol": sym, "period": "5m", "limit": 1},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as resp:
                            if resp.status == 200:
                                now = time.time()
                                data = await resp.json()
                                if data and len(data) > 0:
                                    await writer.insert_taker_buy_sell(
                                        sym, data[0], now
                                    )
                            elif resp.status == 429:
                                log.warning("Taker poller: rate limited, pausing 60s")
                                await asyncio.sleep(60)
                                break
                    except asyncio.CancelledError:
                        return
                    except Exception as e:
                        log.debug(f"Taker error {sym}: {e}")

                    await asyncio.sleep(SENTIMENT_PAUSE_BETWEEN)

        except asyncio.CancelledError:
            return
        except Exception as e:
            log.error(f"Taker poller error: {e}")

        for _ in range(int(TAKER_VOL_POLL_INTERVAL_SECS)):
            if shutdown_event.is_set():
                return
            await asyncio.sleep(1)


# ══════════════════════════════════════════════════════════════════
#  BACKGROUND TASKS
# ══════════════════════════════════════════════════════════════════

async def periodic_flush(writer: AsyncDBWriter, interval: float = 5.0):
    """Flush periódico de buffers."""
    while not shutdown_event.is_set():
        await asyncio.sleep(interval)
        try:
            await writer.flush()
        except Exception as e:
            log.error(f"Flush error: {e}")


async def periodic_heartbeat(writer: AsyncDBWriter, interval: float = 30.0):
    """Heartbeat + stats periódicos."""
    while not shutdown_event.is_set():
        await asyncio.sleep(interval)
        try:
            await writer.heartbeat()
            log.info(
                f"[heartbeat] msgs={writer.total_messages:,} "
                f"pending={writer.pending_count}"
            )
        except Exception as e:
            log.error(f"Heartbeat error: {e}")


async def ws_watchdog(writer: AsyncDBWriter, check_interval: float = 60.0,
                      stale_threshold: float = 120.0):
    """
    Watchdog: si no llegan mensajes WS en stale_threshold segundos,
    cancela todas las tareas WS para forzar reconexión.
    """
    global _ws_last_msg_time
    _ws_last_msg_time = time.time()  # inicializar

    while not shutdown_event.is_set():
        await asyncio.sleep(check_interval)
        if shutdown_event.is_set():
            break

        elapsed = time.time() - _ws_last_msg_time
        if _ws_last_msg_time > 0 and elapsed > stale_threshold:
            log.warning(
                f"[watchdog] ⚠️  Sin mensajes WS hace {elapsed:.0f}s "
                f"(umbral {stale_threshold:.0f}s). Forzando reconexión de "
                f"{len(_ws_tasks)} tareas WS..."
            )
            for t in _ws_tasks:
                if not t.done():
                    t.cancel()
            # Las tareas se re-crearán en run_recorder via el loop de supervisión
            _ws_last_msg_time = time.time()  # reset para no cancelar en loop


# ══════════════════════════════════════════════════════════════════
#  SYSTEMD WATCHDOG
# ══════════════════════════════════════════════════════════════════

def _sd_notify(msg: str):
    """Envía notificación a systemd (READY=1, WATCHDOG=1, etc.)."""
    import socket as _socket
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return
    try:
        sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_DGRAM)
        try:
            if addr[0] == "@":
                addr = "\0" + addr[1:]
            sock.sendto(msg.encode(), addr)
        finally:
            sock.close()
    except Exception:
        pass  # no crítico


async def periodic_sd_watchdog(interval: float = 120.0):
    """Notifica a systemd periódicamente que el proceso está vivo."""
    while not shutdown_event.is_set():
        _sd_notify("WATCHDOG=1")
        await asyncio.sleep(interval)


async def periodic_strategy_eval(strategy: StrategyEngine, interval: float = 5.0):
    """Evalúa la estrategia periódicamente."""
    while not shutdown_event.is_set():
        await asyncio.sleep(interval)
        try:
            now = time.time()
            await strategy.evaluate_all(now)
        except Exception as e:
            log.error(f"Strategy eval error: {e}")


async def periodic_strategy_snapshot(strategy: StrategyEngine, interval: float = 60.0):
    """Graba snapshot de estado de todos los símbolos + persiste AEPS."""
    while not shutdown_event.is_set():
        await asyncio.sleep(interval)
        try:
            now = time.time()
            await strategy.snapshot_all(now)
            strategy.persist_calibrators()
        except Exception as e:
            log.error(f"Strategy snapshot error: {e}")


async def periodic_mfe_persist(strategy: StrategyEngine, interval: float = 10.0):
    """Persiste MFE/MAE de trades abiertos para el dashboard."""
    while not shutdown_event.is_set():
        await asyncio.sleep(interval)
        try:
            await strategy.persist_open_mfe()
        except Exception as e:
            log.error(f"MFE persist error: {e}")


async def periodic_strategy_log(strategy: StrategyEngine, interval: float = 120.0):
    """Log de estado de la estrategia."""
    while not shutdown_event.is_set():
        await asyncio.sleep(interval)
        try:
            log.info(strategy.status_summary())
        except Exception as e:
            log.error(f"Strategy log error: {e}")


async def periodic_status_file(strategy: StrategyEngine, interval: float = 15.0):
    """Write strategy status JSON file for the API/dashboard."""
    while not shutdown_event.is_set():
        await asyncio.sleep(interval)
        try:
            strategy.write_status_file()
        except Exception as e:
            log.error(f"Status file error: {e}")


async def _run_and_send(telegram, strategy):
    """Helper: consulta balance async y envía por Telegram."""
    from telegram_bot import build_balance_text
    text = await build_balance_text(strategy)
    await telegram.send(text)


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

async def run_recorder():
    """Loop principal."""
    # ── Writer (pool) ──
    writer = AsyncDBWriter()
    await writer.connect()

    # ── Init DB (crash recovery check) ──
    crashed = await init_db(writer._pool)
    if crashed:
        log.warning("⚠️  Crash previo detectado — recuperando estado")
    else:
        log.info("DB inicializada (inicio limpio)")

    # ── PID file ──
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

    # ── Descubrir símbolos ──
    symbols = SYMBOLS
    if not symbols:
        log.info("Descubriendo pares USDT-M de Binance Futures...")
        for attempt in range(5):
            try:
                symbols = await fetch_all_futures_symbols()
                break
            except Exception as e:
                log.warning(f"Error descubriendo símbolos (intento {attempt+1}): {e}")
                await asyncio.sleep(5)

    if not symbols:
        log.error("No se pudieron descubrir símbolos. Saliendo.")
        await writer.close()
        return

    log.info(f"Grabando {len(symbols)} pares USDT-M perpetuos")

    # ── Top symbols para sentiment polling ──
    top_symbols = []
    try:
        top_symbols = await fetch_top_symbols_by_volume(SENTIMENT_TOP_N)
        log.info(f"Sentiment polling: top {len(top_symbols)} símbolos por volumen")
    except Exception as e:
        log.warning(f"Error obteniendo top symbols: {e} — usando primeros {SENTIMENT_TOP_N}")
        top_symbols = [s.upper() for s in symbols[:SENTIMENT_TOP_N]]

    # ── Strategy engine ──
    # Actualizar SYMBOLS global para que strategy lo use
    import config
    config.SYMBOLS = symbols

    # ── Binance trader (solo en modo live) ──
    trader = None
    if config.TRADING_MODE == "live":
        from binance_trader import BinanceTrader
        acct = config.BINANCE_ACCOUNTS.get(config.ACTIVE_ACCOUNT)
        if acct:
            trader = BinanceTrader(
                api_key=acct["api_key"],
                api_secret=acct["api_secret"],
                account_name=config.ACTIVE_ACCOUNT,
            )
            await trader.connect()
            bal = await trader.get_account_balance()
            log.warning(
                f"🔴 LIVE MODE — cuenta={config.ACTIVE_ACCOUNT} "
                f"balance=${bal['balance']:,.2f} available=${bal['available']:,.2f}"
            )
        else:
            log.error(f"Cuenta '{config.ACTIVE_ACCOUNT}' no encontrada en BINANCE_ACCOUNTS")
    else:
        log.info("📝 DRY-RUN MODE — trades virtuales, sin conexión a Binance")

    # ── Telegram bot ──
    telegram = None
    if config.TELEGRAM_BOT_TOKEN and config.TELEGRAM_CHAT_ID:
        from telegram_bot import TelegramNotifier
        telegram = TelegramNotifier()
        await telegram.start()

    strategy = StrategyEngine(writer, trader=trader, telegram=telegram)

    # Si arrancamos en live, inicializar equity con balance real de Binance
    if trader and bal:
        live_bal = bal.get("available", 0.0)
        if live_bal > 0:
            for vn in config.VARIANTS:
                strategy.equities["live"][vn] = live_bal
            log.info(f"💰 Live equity inicializado: ${live_bal:,.2f}")

    # Siempre restaurar trades abiertos de la DB (crash o reinicio limpio)
    await strategy.restore_open_trades()

    # ── Stream groups ──
    groups = build_stream_groups(symbols)
    log.info(
        f"Streams: {len(symbols)} × 4 = {len(symbols)*4} market streams → "
        f"{len(groups)} WS + 1 liq WS + OI/funding/LSR/taker pollers"
    )

    # ── Registro de tareas (todas supervisadas y auto-reiniciadas) ──
    task_factories: dict[str, callable] = {}
    active_tasks: dict[str, asyncio.Task] = {}

    def _start_task(name: str, factory: callable):
        task_factories[name] = factory
        active_tasks[name] = asyncio.create_task(factory(), name=name)

    # WebSocket listeners
    for i, streams in enumerate(groups):
        _start_task(
            f"ws-{i}",
            lambda _i=i, _s=streams: ws_listener(_i, _s, writer, strategy),
        )
    _start_task("ws-liq", lambda: ws_force_order_listener(writer))

    # REST pollers
    _start_task("oi-poller", lambda: poll_open_interest(symbols, writer, strategy))
    _start_task("funding-poller", lambda: poll_funding_rates(symbols, writer))
    _start_task("funding-info", lambda: poll_funding_info(strategy))
    if top_symbols:
        _start_task(
            "lsr-poller",
            lambda _ts=top_symbols: poll_long_short_ratio(_ts, writer),
        )
        _start_task(
            "taker-poller",
            lambda _ts=top_symbols: poll_taker_buy_sell(_ts, writer),
        )

    # Background tasks
    _start_task("flush", lambda: periodic_flush(writer))
    _start_task("heartbeat", lambda: periodic_heartbeat(writer))
    _start_task(
        "ws-watchdog",
        lambda: ws_watchdog(writer, check_interval=60, stale_threshold=120),
    )
    _start_task("sd-watchdog", lambda: periodic_sd_watchdog(120.0))
    _start_task("strat-eval", lambda: periodic_strategy_eval(strategy))
    _start_task("strat-snap", lambda: periodic_strategy_snapshot(strategy))
    _start_task("strat-mfe", lambda: periodic_mfe_persist(strategy))
    _start_task("strat-log", lambda: periodic_strategy_log(strategy))
    _start_task("strat-status", lambda: periodic_status_file(strategy))

    # Telegram tasks
    if telegram and telegram.enabled:
        from telegram_bot import (
            telegram_command_loop, periodic_status_report,
            periodic_ip_monitor, fetch_public_ip,
            build_status_text, build_trades_text, build_pnl_text,
            build_balance_text,
        )

        async def _send_ip():
            ip = await fetch_public_ip() or "no disponible"
            await telegram.send(f"🌐 IP pública: <code>{ip}</code>")

        # Register command handlers
        telegram.register_command("status", lambda: telegram.send(build_status_text(strategy)))
        telegram.register_command("trades", lambda: telegram.send(build_trades_text(strategy)))
        telegram.register_command("pnl", lambda: telegram.send(build_pnl_text(strategy)))
        telegram.register_command("balance", lambda: _run_and_send(telegram, strategy))
        telegram.register_command("ip", _send_ip)
        telegram.register_command("help", lambda: telegram.send(
            "📖 <b>Comandos</b>\n"
            "/status — Estado del bot\n"
            "/trades — Trades abiertos\n"
            "/pnl — PnL y equity\n"
            "/balance — Balance Binance\n"
            "/ip — IP pública actual\n"
            "/help — Este mensaje"
        ))
        _start_task("tg-commands", lambda: telegram_command_loop(telegram))
        _start_task("tg-status", lambda: periodic_status_report(
            telegram, strategy, config.TELEGRAM_STATUS_INTERVAL,
        ))
        _start_task("tg-ip-monitor", lambda: periodic_ip_monitor(telegram, 120.0))
        await telegram.notify_startup(
            n_symbols=len(symbols), n_variants=len(config.VARIANTS),
            mode=config.TRADING_MODE, account=config.ACTIVE_ACCOUNT,
            variant=config.ACTIVE_VARIANT,
        )

    # Inicializar refs WS para watchdog
    global _ws_tasks
    _ws_tasks = [
        active_tasks[n] for n in active_tasks if n.startswith("ws-")
    ]

    log.info(f"🚀 Recorder iniciado: {len(active_tasks)} tareas activas")
    _sd_notify("READY=1")

    # ── Supervisor: monitorea y reinicia CUALQUIER tarea muerta ──
    while not shutdown_event.is_set():
        await asyncio.sleep(10)
        if shutdown_event.is_set():
            break

        for name in list(active_tasks):
            task = active_tasks[name]
            if task.done() and not shutdown_event.is_set():
                # Log causa de muerte
                try:
                    exc = task.exception()
                    log.error(
                        f"[supervisor] Tarea '{name}' murió: {exc}\n"
                        f"{''.join(traceback.format_exception(exc))}"
                    )
                except (asyncio.CancelledError, asyncio.InvalidStateError):
                    log.warning(f"[supervisor] Tarea '{name}' cancelada")

                # Re-crear desde factory
                if name in task_factories:
                    new_t = asyncio.create_task(
                        task_factories[name](), name=name,
                    )
                    active_tasks[name] = new_t
                    log.info(f"[supervisor] ✓ Re-creada tarea '{name}'")

        # Actualizar refs WS para watchdog
        _ws_tasks = [
            active_tasks[n] for n in active_tasks if n.startswith("ws-")
        ]

    log.info("Shutdown señalado. Cancelando tareas...")

    # Cancelar todo
    for task in active_tasks.values():
        task.cancel()
    await asyncio.gather(*active_tasks.values(), return_exceptions=True)

    # ── Cleanup ──
    log.info(f"Cerrando. Total mensajes: {writer.total_messages:,}")
    await writer.close()

    # Remove PID file
    try:
        os.remove(PID_FILE)
    except OSError:
        pass

    log.info("✓ Recorder cerrado limpiamente")


def main():
    loop = asyncio.new_event_loop()

    def _shutdown():
        log.info("Señal de parada recibida")
        shutdown_event.set()

    def _handle_exception(loop, context):
        exc = context.get("exception")
        msg = context.get("message", "")
        if exc:
            log.error(
                f"Excepción no capturada en event loop: {msg}\n"
                f"{''.join(traceback.format_exception(exc))}"
            )
        else:
            log.error(f"Excepción no capturada en event loop: {msg}")

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    loop.set_exception_handler(_handle_exception)

    try:
        loop.run_until_complete(run_recorder())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log.critical(f"Error fatal en run_recorder: {e}\n{traceback.format_exc()}")
    finally:
        loop.close()


if __name__ == "__main__":
    main()
