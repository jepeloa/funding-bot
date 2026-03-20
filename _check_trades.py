"""Quick check: trades in the last 10 hours."""
import asyncio, asyncpg, os

# Load .env manually
env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

async def main():
    pw = os.getenv("DB_PASSWORD", "recorder")
    conn = await asyncpg.connect(
        host="localhost", port=5432,
        database="binance_futures", user="recorder", password=pw,
    )

    print("=== Trades (agg_trades) en las últimas 10 horas ===\n")

    # By symbol - use approximate count via grouped query
    rows = await conn.fetch("""
        SELECT symbol, COUNT(*) as total_trades,
               MIN(event_time) as first_trade,
               MAX(event_time) as last_trade
        FROM agg_trades
        WHERE event_time >= NOW() - INTERVAL '10 hours'
        GROUP BY symbol
        ORDER BY total_trades DESC
    """)

    grand_total = sum(r["total_trades"] for r in rows)
    print(f"Total trades: {grand_total:,}")
    print(f"Symbols activos: {len(rows)}\n")

    print(f"{'Symbol':<20} {'Trades':>10} {'First Trade':>28} {'Last Trade':>28}")
    print("-" * 90)
    for r in rows:
        print(f"{r['symbol']:<20} {r['total_trades']:>10,} {str(r['first_trade']):>28} {str(r['last_trade']):>28}")

    # Also check other tables
    for table in ["depth_updates", "book_tickers"]:
        cnt = await conn.fetchval(
            f"SELECT COUNT(*) FROM {table} WHERE event_time >= NOW() - INTERVAL '10 hours'"
        )
        print(f"\n{table}: {cnt:,} registros en las últimas 10h")

    await conn.close()

asyncio.run(main())
