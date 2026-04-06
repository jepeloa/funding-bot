"""
Telegram bot para el recorder de Binance Futures.

Envía notificaciones en tiempo real de trades abiertos/cerrados,
estado del bot, PnL, y responde a comandos interactivos.

Usa aiohttp directamente (sin dependencias extra) contra la API de Telegram.
"""

import asyncio
import json
import logging
import time
from typing import Optional

import aiohttp

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

log = logging.getLogger("telegram")

# ══════════════════════════════════════════════════════════════════
#  Telegram HTTP API (lightweight, no SDK needed)
# ══════════════════════════════════════════════════════════════════

API_BASE = "https://api.telegram.org/bot"


class TelegramNotifier:
    """
    Cliente async de Telegram para notificaciones y comandos.
    Usa aiohttp (ya instalado) — zero dependencias adicionales.
    """

    def __init__(self, token: str = TELEGRAM_BOT_TOKEN,
                 chat_id: str = TELEGRAM_CHAT_ID):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"{API_BASE}{token}"
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_update_id: int = 0
        self._command_handlers: dict = {}
        self.enabled = bool(token and chat_id)

    async def start(self):
        if not self.enabled:
            log.warning("Telegram deshabilitado (sin token o chat_id)")
            return
        self._session = aiohttp.ClientSession()
        me = await self._api("getMe")
        if me:
            log.info(f"🤖 Telegram bot conectado: @{me.get('username', '?')}")
        else:
            log.error("❌ Telegram bot: no se pudo conectar")

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def _api(self, method: str, **kwargs) -> Optional[dict]:
        if not self._session:
            return None
        url = f"{self.base_url}/{method}"
        try:
            async with self._session.post(url, json=kwargs, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                if data.get("ok"):
                    return data.get("result")
                else:
                    log.debug(f"Telegram API error: {data}")
                    return None
        except Exception as e:
            log.debug(f"Telegram API exception: {e}")
            return None

    # ── Envío de mensajes ──

    async def send(self, text: str, parse_mode: str = "HTML"):
        """Envía mensaje al chat configurado."""
        if not self.enabled:
            return
        # Telegram limit: 4096 chars
        if len(text) > 4000:
            text = text[:4000] + "\n…(truncado)"
        await self._api(
            "sendMessage",
            chat_id=self.chat_id,
            text=text,
            parse_mode=parse_mode,
            disable_web_page_preview=True,
        )

    # ── Notificaciones de trades ──

    async def notify_trade_open(self, trade_id: int, symbol: str, variant: str,
                                entry_price: float, notional: float, leverage: int,
                                score: float, energy: float, exhaustion: int,
                                price_change_12h: float, volume_ratio: float,
                                mode: str = "PAPER"):
        text = (
            f"🔴 <b>SHORT #{trade_id}</b>\n"
            f"<code>{symbol}</code> [{variant.upper()}] [{mode}]\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📍 Entrada: <code>${entry_price:.6g}</code>\n"
            f"💰 Nocional: <code>${notional:,.0f}</code> ({leverage}x)\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📊 Ŝ={score:.1f} | E={energy:.1f}h | Ê={exhaustion}\n"
            f"📈 ΔP12h={price_change_12h:+.1%} | V/V̄={volume_ratio:.1f}x"
        )
        await self.send(text)

    async def notify_trade_close(self, trade_id: int, symbol: str, variant: str,
                                 reason: str, pnl_pct: float, pnl_leveraged: float,
                                 pnl_usd: float, hold_hours: float,
                                 mfe: float, mae: float, equity: float,
                                 mode: str = "PAPER"):
        emoji = "🟢" if pnl_usd >= 0 else "🔻"
        text = (
            f"{emoji} <b>CERRADO #{trade_id}</b>\n"
            f"<code>{symbol}</code> [{variant.upper()}] [{mode}]\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📋 Razón: <b>{reason}</b>\n"
            f"💵 PnL: <code>{pnl_pct:+.2%}</code> (x{pnl_leveraged:+.2%})\n"
            f"💰 USD: <code>${pnl_usd:+,.2f}</code>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"⏱ Hold: {hold_hours:.1f}h\n"
            f"📈 MFE: {mfe:.2%} | MAE: {mae:.2%}\n"
            f"🏦 Equity: <code>${equity:,.2f}</code>"
        )
        await self.send(text)

    async def notify_halt(self, variant: str, daily_pnl: float, pct: float):
        text = (
            f"🛑 <b>HALT DIARIO</b> [{variant.upper()}]\n"
            f"Pérdida: <code>${daily_pnl:+,.2f}</code> ({pct:.1%})\n"
            f"Trades suspendidos hasta mañana."
        )
        await self.send(text)

    async def notify_startup(self, n_symbols: int, n_variants: int, mode: str,
                             account: str, variant: str):
        ip = await fetch_public_ip() or "desconocida"
        text = (
            f"🚀 <b>Bot Iniciado</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📡 Símbolos: {n_symbols}\n"
            f"🎯 Variantes: {n_variants}\n"
            f"⚙️ Modo: <b>{mode}</b>\n"
            f"🔑 Cuenta: <b>{account}</b>\n"
            f"📊 Variante activa: <b>{variant}</b>\n"
            f"🌐 IP: <code>{ip}</code>"
        )
        await self.send(text)

    async def notify_config_change(self, old_mode: str, new_mode: str,
                                   old_account: str, new_account: str,
                                   old_variant: str, new_variant: str):
        text = (
            f"⚙️ <b>Config Cambiada</b>\n"
            f"Modo: {old_mode} → <b>{new_mode}</b>\n"
            f"Cuenta: {old_account} → <b>{new_account}</b>\n"
            f"Variante: {old_variant} → <b>{new_variant}</b>"
        )
        await self.send(text)

    # ── Comandos interactivos ──

    def register_command(self, cmd: str, handler):
        """Registra un handler async para /cmd."""
        self._command_handlers[cmd] = handler

    async def poll_commands(self):
        """Long-poll para comandos de Telegram (llamar en loop)."""
        if not self.enabled:
            return
        updates = await self._api(
            "getUpdates",
            offset=self._last_update_id + 1,
            timeout=5,
            allowed_updates=["message"],
        )
        if not updates:
            return
        for u in updates:
            self._last_update_id = u["update_id"]
            msg = u.get("message", {})
            text = msg.get("text", "")
            chat_id = str(msg.get("chat", {}).get("id", ""))
            # Only respond to the configured chat
            if chat_id != str(self.chat_id):
                continue
            if text.startswith("/"):
                cmd = text.split()[0][1:].split("@")[0].lower()
                handler = self._command_handlers.get(cmd)
                if handler:
                    try:
                        await handler()
                    except Exception as e:
                        log.error(f"Telegram cmd /{cmd} error: {e}")
                        await self.send(f"❌ Error en /{cmd}: {e}")


# ══════════════════════════════════════════════════════════════════
#  Command builders — generan texto de respuesta
# ══════════════════════════════════════════════════════════════════

def build_status_text(strategy) -> str:
    """Genera texto de /status con estado actual del bot."""
    from config import VARIANTS
    lines = [
        "📡 <b>Estado del Bot</b>",
        f"⚙️ Modo: <b>{strategy.trading_mode}</b>",
        f"🔑 Cuenta: <b>{strategy.active_account}</b>",
        f"📊 Variante live: <b>{strategy.active_variant}</b>",
        f"🔌 Trader: {'✅ conectado' if strategy.trader else '❌ sin conexión'}",
        "",
    ]

    # Live variant
    av = strategy.active_variant
    if av in VARIANTS:
        n_open = sum(
            1 for vts in strategy.variant_trades[av].values()
            if vts.open_trade_id is not None and vts.trade_mode == "live"
        )
        eq = strategy.equities["live"][av]
        halted = "🛑 HALT" if strategy.halted["live"][av] else "✅"
        daily = strategy.daily_pnl["live"][av]
        lines.append(f"💰 <b>LIVE — {av}</b>: ${eq:,.2f} | {n_open} abiertos | día: ${daily:+,.2f} {halted}")
        lines.append("")

    # Paper variants
    lines.append("📋 <b>PAPER</b>")
    for vname in VARIANTS:
        n_open = sum(
            1 for vts in strategy.variant_trades[vname].values()
            if vts.open_trade_id is not None and vts.trade_mode == "paper"
        )
        eq = strategy.equities["paper"][vname]
        halted = "🛑 HALT" if strategy.halted["paper"][vname] else "✅"
        daily = strategy.daily_pnl["paper"][vname]
        lines.append(
            f"  <b>{vname}</b>: ${eq:,.2f} | "
            f"{n_open} abiertos | día: ${daily:+,.2f} {halted}"
        )

    # Hot symbols
    hot = [
        f"{s.symbol}(Ŝ={s.score:.1f})"
        for s in strategy.states.values()
        if s.score >= 2.0 and s._initialized
    ]
    rec = [s.symbol for s in strategy.states.values() if s.recording]
    init = sum(1 for s in strategy.states.values() if s._initialized)

    lines.append("")
    lines.append(f"📡 Símbolos: {init}/{len(strategy.states)}")
    lines.append(f"🔴 Grabando: {len(rec)}")
    lines.append(f"🔥 Hot: {', '.join(hot[:10]) if hot else 'ninguno'}")

    return "\n".join(lines)


def build_trades_text(strategy) -> str:
    """Genera texto de /trades con los trades abiertos."""
    from config import VARIANTS
    lines = ["📋 <b>Trades Abiertos</b>", ""]
    count = 0
    now = time.time()

    for vname in VARIANTS:
        for sym, vtrade in strategy.variant_trades[vname].items():
            if vtrade.open_trade_id is None:
                continue
            count += 1
            state = strategy.states.get(sym)
            mark = state.mark_price if state else 0
            entry = vtrade.entry_price
            if entry > 0 and mark > 0:
                pnl = (entry - mark) / entry
            else:
                pnl = 0
            hold_h = (now - vtrade.entry_time) / 3600 if vtrade.entry_time > 0 else 0
            mode_tag = "💰" if vtrade.trade_mode == "live" else "📋"
            lines.append(
                f"#{vtrade.open_trade_id} {mode_tag} <code>{sym}</code> [{vname[0].upper()}]\n"
                f"  Entry: ${entry:.6g} → ${mark:.6g}\n"
                f"  PnL: <code>{pnl:+.2%}</code> | MFE: {vtrade.mfe:.2%}\n"
                f"  Hold: {hold_h:.1f}h\n"
            )

    if count == 0:
        lines.append("Sin trades abiertos.")

    return "\n".join(lines)


def build_pnl_text(strategy) -> str:
    """Genera texto de /pnl con resumen de equity y PnL diario."""
    from config import VARIANTS, INITIAL_CAPITAL
    lines = ["💰 <b>PnL & Equity</b>", ""]

    # Live
    av = strategy.active_variant
    if av in VARIANTS:
        eq = strategy.equities["live"][av]
        daily = strategy.daily_pnl["live"][av]
        lines.append(
            f"💰 <b>LIVE — {av}</b>\n"
            f"  Equity: <code>${eq:,.2f}</code>\n"
            f"  Hoy: <code>${daily:+,.2f}</code>"
        )
        lines.append("")

    # Paper
    lines.append("📋 <b>PAPER</b>")
    for vname in VARIANTS:
        eq = strategy.equities["paper"][vname]
        daily = strategy.daily_pnl["paper"][vname]
        total_pnl = eq - INITIAL_CAPITAL
        roi = total_pnl / INITIAL_CAPITAL if INITIAL_CAPITAL > 0 else 0
        lines.append(
            f"<b>{vname}</b>\n"
            f"  Equity: <code>${eq:,.2f}</code>\n"
            f"  Total PnL: <code>${total_pnl:+,.2f}</code> ({roi:+.2%})\n"
            f"  Hoy: <code>${daily:+,.2f}</code>"
        )
        lines.append("")
    return "\n".join(lines)


async def build_balance_text(strategy) -> str:
    """Genera texto de /balance consultando Binance en vivo."""
    from config import BINANCE_ACCOUNTS
    if not strategy.trader:
        return "🔌 Sin conexión a Binance (modo dry-run)"
    try:
        bal = await strategy.trader.get_account_balance()
        positions = await strategy.trader.get_positions()
        lines = [
            f"🏦 <b>Balance Binance</b> [{strategy.active_account}]",
            f"━━━━━━━━━━━━━━━",
            f"💵 Balance: <code>${bal.get('balance', 0):,.2f}</code>",
            f"💰 Disponible: <code>${bal.get('available', 0):,.2f}</code>",
        ]
        if positions:
            lines.append(f"\n📊 <b>Posiciones ({len(positions)})</b>")
            for p in positions[:10]:
                side = p.get("side", "?")
                pnl = float(p.get("pnl", 0))
                emoji = "🟢" if pnl >= 0 else "🔻"
                lines.append(
                    f"  {emoji} {p['symbol']} {side} | "
                    f"PnL: ${pnl:+,.2f}"
                )
        return "\n".join(lines)
    except Exception as e:
        return f"❌ Error consultando balance: {e}"


# ══════════════════════════════════════════════════════════════════
#  Periodic status report
# ══════════════════════════════════════════════════════════════════

async def periodic_status_report(bot: TelegramNotifier, strategy, interval: float = 3600):
    """Envía reporte de estado cada N segundos (default 1h)."""
    import asyncio
    while True:
        await asyncio.sleep(interval)
        try:
            text = build_status_text(strategy)
            await bot.send(text)
        except Exception as e:
            log.error(f"Telegram periodic report error: {e}")


async def telegram_command_loop(bot: TelegramNotifier):
    """Loop de polling de comandos de Telegram."""
    while True:
        try:
            await bot.poll_commands()
        except Exception as e:
            log.debug(f"Telegram poll error: {e}")
        await asyncio.sleep(2)


# ══════════════════════════════════════════════════════════════════
#  IP change monitor
# ══════════════════════════════════════════════════════════════════

_IP_CHECK_URLS = [
    "https://api.ipify.org",
    "https://ifconfig.me/ip",
    "https://icanhazip.com",
]


async def fetch_public_ip() -> Optional[str]:
    """Obtiene la IP pública actual usando múltiples proveedores como fallback."""
    async with aiohttp.ClientSession() as session:
        for url in _IP_CHECK_URLS:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        ip = (await resp.text()).strip()
                        if ip and len(ip) <= 45:  # IPv4 o IPv6 válida
                            return ip
            except Exception:
                continue
    return None


async def periodic_ip_monitor(bot: TelegramNotifier, interval: float = 120.0):
    """
    Monitorea cambios de IP pública cada N segundos (default 2min).
    Notifica por Telegram cuando la IP cambia — indica posible
    reconexión del ISP y pérdida de conexión con Binance.
    """
    current_ip = await fetch_public_ip()
    if current_ip:
        log.info(f"[ip-monitor] IP pública inicial: {current_ip}")
    else:
        log.warning("[ip-monitor] No se pudo obtener IP pública inicial")

    while True:
        await asyncio.sleep(interval)
        try:
            new_ip = await fetch_public_ip()
            if new_ip is None:
                log.warning("[ip-monitor] No se pudo obtener IP pública")
                continue
            if current_ip and new_ip != current_ip:
                log.warning(f"[ip-monitor] IP cambió: {current_ip} → {new_ip}")
                await bot.send(
                    f"⚠️ <b>Cambio de IP detectado</b>\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"Anterior: <code>{current_ip}</code>\n"
                    f"Nueva: <code>{new_ip}</code>\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"🔌 Posible reconexión del ISP.\n"
                    f"WebSockets se reconectarán automáticamente."
                )
            current_ip = new_ip
        except Exception as e:
            log.error(f"[ip-monitor] Error: {e}")
