#!/usr/bin/env python3
"""
Consulta rápida: ¿qué símbolos están cerca de abrir un trade virtual?
Muestra el TOP 20 por score y los candidatos más cercanos a entrada (aggressive).

Uso:  python check_near_entry.py
"""

import asyncio
import os
import asyncpg

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "binance_futures")
DB_USER = os.getenv("DB_USER", "recorder")
DB_PASSWORD = os.getenv("DB_PASSWORD", "K32CzfnWtWtLoj98n6R5QTqEx3jLYLv5")

# Leer .env si existe
ENV_FILE = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(ENV_FILE):
    with open(ENV_FILE) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
    DB_PASSWORD = os.environ.get("DB_PASSWORD", DB_PASSWORD)


async def main():
    conn = await asyncpg.connect(
        host=DB_HOST, port=DB_PORT,
        database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    )

    # ── Trades abiertos ──
    open_trades = await conn.fetch(
        "SELECT * FROM virtual_trades WHERE status='open' ORDER BY entry_time DESC"
    )
    if open_trades:
        print(f"🟢 Trades virtuales ABIERTOS: {len(open_trades)}")
        print("-" * 100)
        for t in open_trades:
            print(dict(t))
        print()
    else:
        print("⚪ No hay trades virtuales abiertos.\n")

    # ── Último snapshot por símbolo ──
    rows = await conn.fetch("""
        SELECT DISTINCT ON (symbol)
            symbol, timestamp, score_total, c_fund, c_oi, c_price, c_taker, c_vol,
            energy_hours, exhaustion,
            funding_rate, taker_buy_ratio, volume_ratio,
            price_change_12h, price_change_24h,
            mark_price, premium_velocity, oi_value, sma_24h
        FROM strategy_snapshots
        ORDER BY symbol, timestamp DESC
    """)

    if not rows:
        print("No hay snapshots de estrategia.")
        await conn.close()
        return

    rows_sorted = sorted(rows, key=lambda r: (r["score_total"] or 0), reverse=True)

    print(f"Total símbolos con snapshots: {len(rows_sorted)}\n")

    # ── TOP 20 por score ──
    print("=== TOP 20 por Score (Ŝ) ===")
    hdr = (
        f"{'Symbol':>16} | {'Score':>5} | {'Energy':>6} | {'Exh':>3} | "
        f"{'cFnd':>4} | {'cOI':>3} | {'cPrc':>4} | {'cTkr':>4} | {'cVol':>4} | "
        f"{'FR':>10} | {'dP12h':>7} | {'η_buy':>5} | {'V/V̄':>5} | "
        f"{'ẋ':>10} | Timestamp"
    )
    print(hdr)
    print("-" * len(hdr) + "-" * 30)
    for r in rows_sorted[:20]:
        s  = r["score_total"]     or 0
        e  = r["energy_hours"]    or 0
        ex = r["exhaustion"]      or 0
        fr = r["funding_rate"]    or 0
        dp = r["price_change_12h"] or 0
        eta = r["taker_buy_ratio"] or 0
        vr = r["volume_ratio"]    or 0
        vel = r["premium_velocity"] or 0
        ts = r["timestamp"]
        print(
            f"{r['symbol']:>16} | {s:>5.1f} | {e:>5.1f}h | {ex:>3} | "
            f"{r['c_fund'] or 0:>4.1f} | {r['c_oi'] or 0:>3.1f} | "
            f"{r['c_price'] or 0:>4.1f} | {r['c_taker'] or 0:>4.1f} | "
            f"{r['c_vol'] or 0:>4.1f} | {fr:>10.6f} | {dp:>6.2%} | "
            f"{eta:>4.1%} | {vr:>5.1f} | {vel:>10.4e} | {ts}"
        )

    # ── Candidatos cercanos a entrada (aggressive) ──
    print()
    print("=== Candidatos cercanos a entrada (variante aggressive) ===")
    print(
        "Condiciones: Ŝ≥2.5, E≥6h, Ê≥2, FR≥0.008%, "
        "c_oi>0 (ΔOI≥3%), ΔP12h≥2%, V/V̄≥2, ẋ≤0\n"
    )

    candidates = []
    for r in rows_sorted:
        s   = r["score_total"]      or 0
        e   = r["energy_hours"]     or 0
        ex  = r["exhaustion"]       or 0
        fr  = r["funding_rate"]     or 0
        dp  = r["price_change_12h"] or 0
        vr  = r["volume_ratio"]     or 0
        vel = r["premium_velocity"] or 0
        c_oi = r["c_oi"]            or 0

        conds = 0
        total = 8
        labels = []

        def chk(ok, tag):
            nonlocal conds
            if ok:
                conds += 1
                labels.append(f"✅{tag}")
            else:
                labels.append(f"❌{tag}")

        chk(s >= 2.5,       "Ŝ")
        chk(e >= 6.0,       "E")
        chk(ex >= 2,        "Ê")
        chk(fr >= 0.00008,  "FR")
        chk(c_oi >= 0.5,    "OI")
        chk(dp >= 0.02,     "ΔP")
        chk(vr >= 2.0,      "V")
        chk(vel <= 0,       "ẋ")

        if conds >= 3:
            candidates.append((conds, r, labels))

    candidates.sort(key=lambda x: x[0], reverse=True)

    if not candidates:
        print("Ningún símbolo cumple ≥3/8 condiciones.")
    else:
        for conds, r, labels in candidates[:20]:
            s  = r["score_total"]      or 0
            e  = r["energy_hours"]     or 0
            ex = r["exhaustion"]       or 0
            fr = r["funding_rate"]     or 0
            dp = r["price_change_12h"] or 0
            vr = r["volume_ratio"]     or 0
            print(
                f"{r['symbol']:>16} [{conds}/8] {' '.join(labels)}  |  "
                f"Ŝ={s:.1f} E={e:.1f}h Ê={ex} "
                f"FR={fr:.5f} ΔP={dp:.2%} V/V̄={vr:.1f}"
            )

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
