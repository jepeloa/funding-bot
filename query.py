"""
Consultas sobre los datos grabados de Binance Futures (TimescaleDB).

Uso:  python3 query.py [comando] [opciones]

Comandos:
  stats                          → Resumen general de la DB
  trades  SYMBOL [--limit N]     → Últimos aggTrades
  depth   SYMBOL [--limit N]     → Últimos depth updates
  tickers SYMBOL [--limit N]     → Últimos book tickers
  marks   SYMBOL [--limit N]     → Últimos mark prices
  oi      SYMBOL [--limit N]     → Último open interest
  ohlcv   SYMBOL INTERVAL        → Velas OHLCV (1m, 1h, 1d)
  spread  SYMBOL [--limit N]     → Análisis de spread bid-ask  liq     [SYMBOL] [--limit N]   → Liquidaciones recientes
  lsr     SYMBOL [--limit N]     → Long/Short Ratio (top + global)
  taker   SYMBOL [--limit N]     → Taker Buy/Sell Volume  vtrades [--status open|closed] → Virtual trades de la estrategia
  pnl                            → Resumen de PnL de la estrategia
  snapshots SYMBOL [--limit N]   → Strategy snapshots
  symbols                        → Lista de símbolos grabados
  export  TABLE  FILE.csv        → Exportar tabla a CSV
  dbsize                         → Tamaño detallado por hypertable
"""

import argparse
import csv
import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


def get_conn():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = True
    return conn


def fmt_ts(dt) -> str:
    """Formatea un datetime o timestamp."""
    if dt is None:
        return "N/A"
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    # epoch float
    return datetime.fromtimestamp(dt, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def fmt_epoch(epoch: float) -> str:
    if epoch is None:
        return "N/A"
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# ══════════════════════════════════════════════════════════════════
#  COMANDOS
# ══════════════════════════════════════════════════════════════════

def cmd_stats():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    tables = [
        ("depth_updates", "event_time"),
        ("agg_trades", "event_time"),
        ("book_tickers", "event_time"),
        ("mark_prices", "event_time"),
        ("open_interest", "polled_at"),
        ("funding_rates", "funding_time"),
        ("strategy_snapshots", "timestamp"),
    ]

    print(f"\n{'═'*65}")
    print(f"  BINANCE FUTURES RECORDER — ESTADÍSTICAS (TimescaleDB)")
    print(f"{'═'*65}")

    for table, ts_col in tables:
        try:
            # Usar approximate_row_count para hypertables (instantáneo)
            cur.execute(
                "SELECT approximate_row_count(%s::regclass)",
                (table,),
            )
            cnt = cur.fetchone()[0]
            print(f"\n  {table}: ~{cnt:,} registros (aprox)")

            if cnt > 0:
                cur.execute(
                    f"SELECT MIN({ts_col}), MAX({ts_col}) FROM {table}"
                )
                row = cur.fetchone()
                if row[0] is not None:
                    print(f"    Desde: {fmt_ts(row[0])}")
                    print(f"    Hasta: {fmt_ts(row[1])}")
        except Exception as e:
            print(f"\n  {table}: error - {e}")

    # Virtual trades (tabla regular, COUNT exacto es rápido)
    try:
        cur.execute("SELECT COUNT(*) FROM virtual_trades WHERE status='open'")
        open_t = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM virtual_trades WHERE status='closed'")
        closed_t = cur.fetchone()[0]
        print(f"\n  virtual_trades: {open_t} abiertos, {closed_t} cerrados")
    except Exception:
        pass

    # Símbolos
    try:
        cur.execute("SELECT DISTINCT symbol FROM agg_trades ORDER BY symbol LIMIT 500")
        syms = cur.fetchall()
        print(f"\n{'─'*65}")
        print(f"  Símbolos grabados: {len(syms)}")
        if len(syms) <= 20:
            print(f"  {', '.join(r[0] for r in syms)}")
    except Exception:
        pass

    # Tamaño DB
    try:
        cur.execute("SELECT pg_database_size(%s)", (DB_NAME,))
        size_bytes = cur.fetchone()[0]
        size_mb = size_bytes / (1024 * 1024)
        size_gb = size_mb / 1024
        print(f"\n{'─'*65}")
        print(f"  Tamaño DB total: {size_gb:.2f} GB ({size_mb:.0f} MB)")
    except Exception:
        pass

    print(f"{'═'*65}\n")
    cur.close()
    conn.close()


def cmd_dbsize():
    """Tamaño detallado por hypertable con compresión."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    print(f"\n{'═'*80}")
    print(f"  TAMAÑO POR HYPERTABLE")
    print(f"{'═'*80}")
    print(f"  {'Table':<25s} {'Total':>10s} {'Uncompressed':>14s} {'Compressed':>12s} {'Ratio':>7s}")
    print(f"  {'─'*25} {'─'*10} {'─'*14} {'─'*12} {'─'*7}")

    try:
        cur.execute("""
            SELECT
                hypertable_name,
                pg_size_pretty(hypertable_size(format('%I', hypertable_name)::regclass)) as total,
                pg_size_pretty(before_compression_total_bytes) as before_comp,
                pg_size_pretty(after_compression_total_bytes) as after_comp,
                CASE WHEN after_compression_total_bytes > 0
                     THEN round(before_compression_total_bytes::numeric /
                                after_compression_total_bytes, 1)
                     ELSE 0 END as ratio
            FROM timescaledb_information.hypertables h
            LEFT JOIN LATERAL (
                SELECT * FROM hypertable_compression_stats(
                    format('%I', h.hypertable_name)::regclass
                )
            ) cs ON true
            ORDER BY hypertable_size(format('%I', hypertable_name)::regclass) DESC
        """)
        for row in cur.fetchall():
            ratio_str = f"{row['ratio']}x" if row['ratio'] and row['ratio'] > 0 else "N/A"
            before = row['before_comp'] or "N/A"
            after = row['after_comp'] or "N/A"
            print(f"  {row['hypertable_name']:<25s} {row['total']:>10s} {before:>14s} {after:>12s} {ratio_str:>7s}")
    except Exception as e:
        print(f"  Error: {e}")

    # Total
    try:
        cur.execute("SELECT pg_size_pretty(pg_database_size(%s))", (DB_NAME,))
        total = cur.fetchone()[0]
        print(f"\n  Total database: {total}")
    except Exception:
        pass

    print(f"{'═'*80}\n")
    cur.close()
    conn.close()


def cmd_trades(symbol: str, limit: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(
        "SELECT event_time, agg_trade_id, price, quantity, is_buyer_maker "
        "FROM agg_trades WHERE symbol = %s ORDER BY event_time DESC LIMIT %s",
        (symbol.upper(), limit),
    )
    rows = cur.fetchall()
    for r in reversed(rows):
        side = "SELL" if r["is_buyer_maker"] else "BUY "
        print(
            f"[{fmt_ts(r['event_time'])}] {side} "
            f"price={r['price']:>12.6f}  qty={r['quantity']:>14.6f}  "
            f"id={r['agg_trade_id']}"
        )
    print(f"\n({len(rows)} registros)")
    cur.close()
    conn.close()


def cmd_depth(symbol: str, limit: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(
        "SELECT event_time, last_update_id, bid_prices, bid_qtys, ask_prices, ask_qtys "
        "FROM depth_updates WHERE symbol = %s ORDER BY event_time DESC LIMIT %s",
        (symbol.upper(), limit),
    )
    rows = cur.fetchall()
    for r in reversed(rows):
        bids_top3 = list(zip(r["bid_prices"][:3], r["bid_qtys"][:3]))
        asks_top3 = list(zip(r["ask_prices"][:3], r["ask_qtys"][:3]))
        print(f"\n[{fmt_ts(r['event_time'])}] updateId={r['last_update_id']}")
        print(f"  Top bids: {bids_top3}")
        print(f"  Top asks: {asks_top3}")
    print(f"\n({len(rows)} registros)")
    cur.close()
    conn.close()


def cmd_tickers(symbol: str, limit: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(
        "SELECT event_time, best_bid_price, best_bid_qty, "
        "best_ask_price, best_ask_qty "
        "FROM book_tickers WHERE symbol = %s ORDER BY event_time DESC LIMIT %s",
        (symbol.upper(), limit),
    )
    rows = cur.fetchall()
    for r in reversed(rows):
        spread = r["best_ask_price"] - r["best_bid_price"]
        print(
            f"[{fmt_ts(r['event_time'])}] "
            f"bid={r['best_bid_price']:>12.6f} ({r['best_bid_qty']:>12.6f})  "
            f"ask={r['best_ask_price']:>12.6f} ({r['best_ask_qty']:>12.6f})  "
            f"spread={spread:.6f}"
        )
    print(f"\n({len(rows)} registros)")
    cur.close()
    conn.close()


def cmd_marks(symbol: str, limit: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(
        "SELECT event_time, mark_price, index_price, funding_rate "
        "FROM mark_prices WHERE symbol = %s ORDER BY event_time DESC LIMIT %s",
        (symbol.upper(), limit),
    )
    rows = cur.fetchall()
    for r in reversed(rows):
        premium = r["mark_price"] - r["index_price"]
        print(
            f"[{fmt_ts(r['event_time'])}] "
            f"mark={r['mark_price']:>12.6f}  "
            f"index={r['index_price']:>12.6f}  "
            f"premium={premium:+.6f}  "
            f"funding={r['funding_rate']:.6f}"
        )
    print(f"\n({len(rows)} registros)")
    cur.close()
    conn.close()


def cmd_oi(symbol: str, limit: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(
        "SELECT polled_at, oi_contracts, oi_value "
        "FROM open_interest WHERE symbol = %s ORDER BY polled_at DESC LIMIT %s",
        (symbol.upper(), limit),
    )
    rows = cur.fetchall()
    for r in reversed(rows):
        print(
            f"[{fmt_ts(r['polled_at'])}] "
            f"contracts={r['oi_contracts']:>14,.2f}  "
            f"value=${r['oi_value']:>16,.2f}"
        )
    print(f"\n({len(rows)} registros)")
    cur.close()
    conn.close()


def cmd_ohlcv(symbol: str, interval: str):
    """Usa continuous aggregates precalculados."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Mapear intervalo a vista
    view_map = {
        "1m": "ohlcv_1m",
        "1min": "ohlcv_1m",
        "1h": "ohlcv_1h",
        "1hour": "ohlcv_1h",
        "1d": "ohlcv_1d",
        "1day": "ohlcv_1d",
    }

    view = view_map.get(interval.lower())
    if not view:
        print(f"Intervalo '{interval}' no soportado. Opciones: {', '.join(view_map.keys())}")
        return

    cur.execute(
        f"SELECT bucket, open, high, low, close, volume, volume_usdt, trade_count "
        f"FROM {view} WHERE symbol = %s "
        f"ORDER BY bucket DESC LIMIT 100",
        (symbol.upper(),),
    )
    rows = cur.fetchall()

    if not rows:
        print("Sin datos.")
        return

    print(f"{'Timestamp':>23s} {'Open':>12s} {'High':>12s} {'Low':>12s} "
          f"{'Close':>12s} {'Volume':>14s} {'#Trades':>8s}")
    print("─" * 100)
    for r in reversed(rows):
        print(
            f"{fmt_ts(r['bucket']):>23s} {r['open']:12.4f} {r['high']:12.4f} "
            f"{r['low']:12.4f} {r['close']:12.4f} {r['volume']:14.6f} "
            f"{r['trade_count']:8d}"
        )
    print(f"\n({len(rows)} velas {interval})")
    cur.close()
    conn.close()


def cmd_spread(symbol: str, limit: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(
        "SELECT event_time, best_ask_price - best_bid_price as spread, "
        "best_bid_price as bid "
        "FROM book_tickers WHERE symbol = %s "
        "ORDER BY event_time DESC LIMIT %s",
        (symbol.upper(), limit),
    )
    rows = cur.fetchall()

    if not rows:
        print("Sin datos.")
        return

    spreads = [r["spread"] for r in rows]
    avg = sum(spreads) / len(spreads)
    print(f"Spread {symbol.upper()} (últimos {len(rows)} ticks):")
    print(f"  Promedio : {avg:.6f}")
    print(f"  Mínimo   : {min(spreads):.6f}")
    print(f"  Máximo   : {max(spreads):.6f}")
    if rows[0]["bid"] > 0:
        print(f"  Spread % : {(avg / rows[0]['bid']) * 100:.6f}%")
    cur.close()
    conn.close()


def cmd_vtrades(status: str | None):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    if status:
        cur.execute(
            "SELECT * FROM virtual_trades WHERE status=%s ORDER BY entry_time DESC",
            (status,),
        )
    else:
        cur.execute(
            "SELECT * FROM virtual_trades ORDER BY entry_time DESC"
        )
    rows = cur.fetchall()

    if not rows:
        print("Sin virtual trades.")
        return

    for r in rows:
        emoji = "🟢" if (r["pnl_usd"] or 0) >= 0 else "🔻"
        status_str = r["status"].upper()
        variant = r["variant"] if r["variant"] else "?"
        line = (
            f"#{r['id']:>4d} [{status_str:>6s}] [{variant:>12s}] {r['symbol']:>14s} | "
            f"entry={fmt_epoch(r['entry_time'])} @ ${r['entry_price']:.6f}"
        )
        if r["exit_time"]:
            line += (
                f" → exit={fmt_epoch(r['exit_time'])} @ ${r['exit_price']:.6f} | "
                f"{emoji} PnL={r['pnl_pct']:+.2%} (x{r['leverage']}={r['pnl_leveraged']:+.2%}) "
                f"${r['pnl_usd']:+,.2f} | {r['exit_reason']} | "
                f"{r['hold_hours']:.1f}h"
            )
        print(line)

    print(f"\n({len(rows)} trades)")
    cur.close()
    conn.close()


def cmd_pnl():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(
        "SELECT * FROM virtual_trades WHERE status='closed' ORDER BY exit_time"
    )
    rows = cur.fetchall()

    if not rows:
        print("Sin trades cerrados.")
        return

    # ── Resumen global ──
    total_pnl = sum(r["pnl_usd"] for r in rows)
    wins = [r for r in rows if r["pnl_usd"] >= 0]
    losses = [r for r in rows if r["pnl_usd"] < 0]
    win_rate = len(wins) / len(rows) * 100 if rows else 0

    print(f"\n{'═'*70}")
    print(f"  RESUMEN DE PnL — Estrategia α_f Bifurcation Short (4 variantes)")
    print(f"{'═'*70}")
    print(f"  Trades cerrados : {len(rows)}")
    print(f"  Ganadores       : {len(wins)} ({win_rate:.1f}%)")
    print(f"  Perdedores      : {len(losses)} ({100-win_rate:.1f}%)")
    print(f"  PnL total       : ${total_pnl:+,.2f}")
    if wins:
        print(f"  Mejor trade     : ${max(r['pnl_usd'] for r in wins):+,.2f}")
    if losses:
        print(f"  Peor trade      : ${min(r['pnl_usd'] for r in losses):+,.2f}")
    avg_hold = sum(r["hold_hours"] for r in rows) / len(rows)
    print(f"  Hold promedio   : {avg_hold:.1f}h")

    # ── Por variante ──
    variants = {}
    for r in rows:
        v = r["variant"] or "unknown"
        if v not in variants:
            variants[v] = {"count": 0, "wins": 0, "pnl": 0.0}
        variants[v]["count"] += 1
        variants[v]["pnl"] += r["pnl_usd"]
        if r["pnl_usd"] >= 0:
            variants[v]["wins"] += 1

    print(f"\n  Por variante:")
    for v in ["conservative", "base", "aggressive", "high_energy"]:
        if v not in variants:
            continue
        d = variants[v]
        wr = d["wins"] / d["count"] * 100 if d["count"] else 0
        print(
            f"    {v:>14s}: {d['count']:>3d} trades, "
            f"WR={wr:.0f}%, PnL ${d['pnl']:+,.2f}"
        )
    # Any unknown variants
    for v, d in variants.items():
        if v not in ("conservative", "base", "aggressive", "high_energy"):
            wr = d["wins"] / d["count"] * 100 if d["count"] else 0
            print(
                f"    {v:>14s}: {d['count']:>3d} trades, "
                f"WR={wr:.0f}%, PnL ${d['pnl']:+,.2f}"
            )

    # Por razón de cierre
    reasons = {}
    for r in rows:
        reason = r["exit_reason"]
        if reason not in reasons:
            reasons[reason] = {"count": 0, "pnl": 0.0}
        reasons[reason]["count"] += 1
        reasons[reason]["pnl"] += r["pnl_usd"]

    print(f"\n  Por razón de cierre:")
    for reason, data in sorted(reasons.items()):
        print(
            f"    {reason:>15s}: {data['count']:>3d} trades, "
            f"PnL ${data['pnl']:+,.2f}"
        )

    # Open trades
    cur.execute(
        "SELECT COUNT(*) FROM virtual_trades WHERE status='open'"
    )
    open_count = cur.fetchone()[0]
    if open_count > 0:
        print(f"\n  ⚠️  Trades abiertos: {open_count}")

    print(f"{'═'*70}\n")
    cur.close()
    conn.close()


def cmd_snapshots(symbol: str, limit: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(
        "SELECT * FROM strategy_snapshots "
        "WHERE symbol = %s ORDER BY timestamp DESC LIMIT %s",
        (symbol.upper(), limit),
    )
    rows = cur.fetchall()
    for r in reversed(rows):
        print(
            f"[{fmt_ts(r['timestamp'])}] {r['symbol']} | "
            f"Ŝ={r['score_total']:.1f} "
            f"[f={r['c_fund']:.1f},oi={r['c_oi']:.1f},"
            f"p={r['c_price']:.1f},t={r['c_taker']:.1f},v={r['c_vol']:.1f}] | "
            f"E={r['energy_hours']:.1f}h Ê={r['exhaustion']} | "
            f"P=${r['mark_price']:.4f}"
        )
    print(f"\n({len(rows)} snapshots)")
    cur.close()
    conn.close()


def cmd_symbols():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(
        "SELECT DISTINCT symbol FROM agg_trades ORDER BY symbol"
    )
    rows = cur.fetchall()
    for r in rows:
        print(f"  {r['symbol']:>14s}")
    print(f"\n({len(rows)} símbolos)")
    cur.close()
    conn.close()


def cmd_liquidations(symbol: str | None, limit: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if symbol:
        cur.execute(
            "SELECT event_time, symbol, side, original_qty, price, avg_price, "
            "filled_qty, order_status FROM liquidations "
            "WHERE symbol = %s ORDER BY event_time DESC LIMIT %s",
            (symbol.upper(), limit),
        )
    else:
        cur.execute(
            "SELECT event_time, symbol, side, original_qty, price, avg_price, "
            "filled_qty, order_status FROM liquidations "
            "ORDER BY event_time DESC LIMIT %s",
            (limit,),
        )

    rows = cur.fetchall()
    if not rows:
        print("Sin liquidaciones registradas.")
        cur.close()
        conn.close()
        return

    title = f"Liquidaciones{' ' + symbol.upper() if symbol else ' (todas)'}  [{len(rows)} más recientes]"
    print(f"\n  {title}")
    print(f"  {'='*90}")
    print(f"  {'Tiempo':22s} {'Símbolo':>14s} {'Side':>5s} {'Qty':>12s} "
          f"{'Bankruptcy':>12s} {'Avg Price':>12s} {'Filled':>12s} {'Status':>10s}")
    print(f"  {'-'*90}")

    for r in rows:
        print(f"  {fmt_ts(r['event_time']):22s} {r['symbol']:>14s} {r['side']:>5s} "
              f"{r['original_qty']:>12.4f} {r['price']:>12.2f} "
              f"{r['avg_price']:>12.2f} {r['filled_qty']:>12.4f} {r['order_status']:>10s}")

    # Resumen
    cur.execute(
        "SELECT COUNT(*) as total, "
        "SUM(CASE WHEN side='SELL' THEN 1 ELSE 0 END) as long_liq, "
        "SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END) as short_liq "
        "FROM liquidations" + (" WHERE symbol = %s" if symbol else ""),
        (symbol.upper(),) if symbol else None,
    )
    s = cur.fetchone()
    if s:
        print(f"\n  Total histórico: {s['total']:,} liquidaciones "
              f"(longs: {s['long_liq']:,}, shorts: {s['short_liq']:,})")

    cur.close()
    conn.close()


def cmd_lsr(symbol: str, limit: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(
        "SELECT timestamp, ratio_type, long_short_ratio, "
        "long_account_pct, short_account_pct "
        "FROM long_short_ratio "
        "WHERE symbol = %s ORDER BY timestamp DESC LIMIT %s",
        (symbol.upper(), limit),
    )
    rows = cur.fetchall()
    if not rows:
        print(f"Sin datos L/S ratio para {symbol.upper()}.")
        cur.close()
        conn.close()
        return

    print(f"\n  Long/Short Ratio — {symbol.upper()}  [{len(rows)} más recientes]")
    print(f"  {'='*80}")
    print(f"  {'Tiempo':22s} {'Tipo':>15s} {'L/S Ratio':>10s} {'Long %':>8s} {'Short %':>8s}")
    print(f"  {'-'*80}")

    for r in rows:
        print(f"  {fmt_ts(r['timestamp']):22s} {r['ratio_type']:>15s} "
              f"{r['long_short_ratio']:>10.4f} {r['long_account_pct']:>8.2%} "
              f"{r['short_account_pct']:>8.2%}")

    cur.close()
    conn.close()


def cmd_taker(symbol: str, limit: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(
        "SELECT timestamp, buy_sell_ratio, buy_vol, sell_vol "
        "FROM taker_buy_sell "
        "WHERE symbol = %s ORDER BY timestamp DESC LIMIT %s",
        (symbol.upper(), limit),
    )
    rows = cur.fetchall()
    if not rows:
        print(f"Sin datos taker buy/sell para {symbol.upper()}.")
        cur.close()
        conn.close()
        return

    print(f"\n  Taker Buy/Sell Volume — {symbol.upper()}  [{len(rows)} más recientes]")
    print(f"  {'='*80}")
    print(f"  {'Tiempo':22s} {'B/S Ratio':>10s} {'Buy Vol':>16s} {'Sell Vol':>16s}")
    print(f"  {'-'*80}")

    for r in rows:
        print(f"  {fmt_ts(r['timestamp']):22s} {r['buy_sell_ratio']:>10.4f} "
              f"{r['buy_vol']:>16.2f} {r['sell_vol']:>16.2f}")

    cur.close()
    conn.close()


def cmd_export(table: str, filepath: str):
    valid = (
        "depth_updates", "agg_trades", "book_tickers", "mark_prices",
        "open_interest", "funding_rates", "virtual_trades", "strategy_snapshots",
        "liquidations", "long_short_ratio", "taker_buy_sell",
    )
    if table not in valid:
        print(f"Tabla inválida. Opciones: {valid}")
        return

    conn = get_conn()
    cur = conn.cursor()

    # Usar COPY TO STDOUT para export eficiente
    with open(filepath, "w", newline="") as f:
        cur.copy_expert(
            f"COPY {table} TO STDOUT WITH CSV HEADER",
            f,
        )

    # Contar líneas para feedback
    with open(filepath) as f:
        lines = sum(1 for _ in f) - 1  # minus header

    print(f"Exportados {lines:,} registros → {filepath}")
    cur.close()
    conn.close()


# ══════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Consulta datos de Binance Futures (TimescaleDB)")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("stats")
    sub.add_parser("symbols")
    sub.add_parser("pnl")
    sub.add_parser("dbsize")

    for cmd in ("trades", "depth", "tickers", "marks", "oi", "snapshots"):
        p = sub.add_parser(cmd)
        p.add_argument("symbol")
        p.add_argument("--limit", type=int, default=20)

    p = sub.add_parser("liq")
    p.add_argument("symbol", nargs="?", default=None)
    p.add_argument("--limit", type=int, default=30)

    for cmd in ("lsr", "taker"):
        p = sub.add_parser(cmd)
        p.add_argument("symbol")
        p.add_argument("--limit", type=int, default=20)

    p = sub.add_parser("ohlcv")
    p.add_argument("symbol")
    p.add_argument("interval", help="1m, 1h, or 1d")

    p = sub.add_parser("spread")
    p.add_argument("symbol")
    p.add_argument("--limit", type=int, default=1000)

    p = sub.add_parser("vtrades")
    p.add_argument("--status", choices=["open", "closed"])

    p = sub.add_parser("export")
    p.add_argument("table")
    p.add_argument("file")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return

    match args.command:
        case "stats":
            cmd_stats()
        case "symbols":
            cmd_symbols()
        case "pnl":
            cmd_pnl()
        case "dbsize":
            cmd_dbsize()
        case "trades":
            cmd_trades(args.symbol, args.limit)
        case "depth":
            cmd_depth(args.symbol, args.limit)
        case "tickers":
            cmd_tickers(args.symbol, args.limit)
        case "marks":
            cmd_marks(args.symbol, args.limit)
        case "oi":
            cmd_oi(args.symbol, args.limit)
        case "ohlcv":
            cmd_ohlcv(args.symbol, args.interval)
        case "spread":
            cmd_spread(args.symbol, args.limit)
        case "vtrades":
            cmd_vtrades(args.status)
        case "snapshots":
            cmd_snapshots(args.symbol, args.limit)
        case "liq":
            cmd_liquidations(args.symbol, args.limit)
        case "lsr":
            cmd_lsr(args.symbol, args.limit)
        case "taker":
            cmd_taker(args.symbol, args.limit)
        case "export":
            cmd_export(args.table, args.file)


if __name__ == "__main__":
    main()
