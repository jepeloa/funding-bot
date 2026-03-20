"""Test all 3 copy-trading endpoints after refactor + verify field consistency."""
import asyncio
import json
import sys
import time
sys.path.insert(0, "/home/javier/mcp_data_crypto/src")

from psi_jam_mcp.copytrading_client import CopyTradingClient

# Fields that should be consistent across endpoints
EXPECTED_POSITION_FIELDS = {"symbol", "direction", "amount", "entry_price", "mark_price", "unrealized_pnl_usdt", "leverage", "isolated"}
EXPECTED_SUMMARY_FIELDS = {"portfolio_id", "nickname", "roi_pct", "pnl_usdt", "aum_usdt", "mdd_pct", "win_rate_pct", "sharp_ratio", "copiers", "max_copiers", "slots_available", "badge", "portfolio_type"}
EXPECTED_DETAIL_STATS_FIELDS = {"margin_balance_usdt", "aum_usdt", "copier_pnl_usdt", "profit_sharing_pct", "current_copiers", "max_copiers", "total_copiers_historical", "favorites", "win_rate_pct", "mdd_pct", "sharp_ratio"}

def check_fields(data, expected, label):
    actual = set(data.keys())
    missing = expected - actual
    extra = actual - expected
    ok = not missing
    if missing:
        print(f"  FAIL {label}: missing fields: {missing}")
    if extra:
        print(f"  INFO {label}: extra fields (ok): {extra}")
    if ok and not extra:
        print(f"  OK   {label}: all fields match")
    elif ok:
        print(f"  OK   {label}: all expected fields present")
    return ok

async def test():
    client = CopyTradingClient()
    all_ok = True
    try:
        # ── TEST 1: search_top_traders ──
        print("=" * 70)
        print("TEST 1: search_top_traders (JS fetch, ROI, 30D)")
        print("=" * 70)
        t0 = time.time()
        result1 = await client.search_top_traders(page_size=5, sort_by="ROI", time_range="30D")
        elapsed = time.time() - t0
        print(f"  Time: {elapsed:.1f}s")
        print(f"  Total: {result1.get('total', 'N/A')}")
        print(f"  Traders: {len(result1.get('traders', []))}")
        print(f"  Daily picks: {len(result1.get('daily_picks', []))}")
        print(f"  Query echoed: {result1.get('query', {})}")

        if result1.get("traders"):
            t = result1["traders"][0]
            print(f"\n  Top trader: {t['nickname']} ROI={t['roi_pct']:.2f}%")
            ok = check_fields(t, EXPECTED_SUMMARY_FIELDS, "trader summary")
            all_ok = all_ok and ok

            # Check types: _pct should be numeric, _usdt should be numeric
            for field in ["roi_pct", "pnl_usdt", "aum_usdt", "mdd_pct", "win_rate_pct"]:
                val = t.get(field)
                if val is not None and not isinstance(val, (int, float)):
                    print(f"  FAIL type: {field}={val!r} is {type(val).__name__}, expected numeric")
                    all_ok = False
        else:
            print("  FAIL: no traders returned")
            all_ok = False

        # ── TEST 2: get_trader_detail ──
        print("\n" + "=" * 70)
        print("TEST 2: get_trader_detail (parallel JS fetch)")
        print("=" * 70)
        t0 = time.time()
        result2 = await client.get_trader_detail(portfolio_id="4878630112238695169", time_range="7D")
        elapsed = time.time() - t0
        print(f"  Time: {elapsed:.1f}s")

        if result2.get("profile"):
            p = result2["profile"]
            s = result2.get("stats", {})
            print(f"  Trader: {p['nickname']} ({p.get('nickname_translate', '')})")
            print(f"  Status: {p['status']} | Badge: {p.get('badge')}")
            ok = check_fields(s, EXPECTED_DETAIL_STATS_FIELDS, "detail stats")
            all_ok = all_ok and ok

            # ── KEY CHECK: field naming consistency ──
            # stats should use win_rate_pct (not win_rate), mdd_pct (not mdd)
            if "win_rate" in s:
                print("  FAIL consistency: stats has 'win_rate' instead of 'win_rate_pct'")
                all_ok = False
            if "mdd" in s and "mdd_pct" not in s:
                print("  FAIL consistency: stats has 'mdd' instead of 'mdd_pct'")
                all_ok = False
        else:
            print(f"  FAIL: {result2.get('error', 'no profile')}")
            all_ok = False

        if result2.get("roi_chart"):
            print(f"  ROI chart: {len(result2['roi_chart'])} points")
        else:
            print("  WARN: no ROI chart data")

        if result2.get("coin_distribution"):
            print(f"  Coin distribution: {len(result2['coin_distribution'])} coins")
        else:
            print("  WARN: no coin distribution data")

        if result2.get("open_positions"):
            pos = result2["open_positions"][0]
            print(f"  Open positions: {len(result2['open_positions'])}")
            ok = check_fields(pos, EXPECTED_POSITION_FIELDS, "position (detail)")
            all_ok = all_ok and ok

            # Check: should have 'direction' not 'side'
            if "side" in pos:
                print("  FAIL consistency: position has 'side' instead of 'direction'")
                all_ok = False
            # Check: 'amount' should be float
            if not isinstance(pos.get("amount"), float):
                print(f"  FAIL type: amount={pos.get('amount')!r} is not float")
                all_ok = False
            # Check: 'unrealized_pnl_usdt' not 'unrealized_pnl'
            if "unrealized_pnl" in pos and "unrealized_pnl_usdt" not in pos:
                print("  FAIL consistency: has 'unrealized_pnl' instead of 'unrealized_pnl_usdt'")
                all_ok = False
        else:
            print(f"  Positions: none active (symbols configured: {result2.get('total_position_symbols', 0)})")

        # ── TEST 3: scan_symbol_positions ──
        print("\n" + "=" * 70)
        print("TEST 3: scan_symbol_positions (BTCUSDT, top 50)")
        print("=" * 70)
        t0 = time.time()
        result3 = await client.scan_symbol_positions(symbol="BTCUSDT", top_n=50, sort_by="AUM")
        elapsed = time.time() - t0
        print(f"  Time: {elapsed:.1f}s")
        print(f"  Scanned: {result3['scan_config']['top_n_traders_scanned']} traders")

        s3 = result3["summary"]
        print(f"  With position: {s3['traders_with_position']}")
        print(f"  LONG:  {s3['long_count']} | ${s3['total_long_notional_usdt']:,.0f}")
        print(f"  SHORT: {s3['short_count']} | ${s3['total_short_notional_usdt']:,.0f}")
        print(f"  Bias: {s3['net_bias']} {s3['bias_ratio']}x")

        # Check scan position record fields are consistent with detail positions
        all_records = result3.get("longs", []) + result3.get("shorts", [])
        if all_records:
            rec = all_records[0]
            # Scan records have extra fields (portfolio_id, nickname, aum_usdt, etc.)
            # but the position-specific fields should match
            for field in ["direction", "amount", "entry_price", "mark_price", "unrealized_pnl_usdt", "leverage"]:
                if field not in rec:
                    print(f"  FAIL consistency: scan record missing '{field}'")
                    all_ok = False
            if "side" in rec:
                print("  FAIL consistency: scan record has 'side' instead of 'direction'")
                all_ok = False
            if not isinstance(rec.get("amount"), float):
                print(f"  FAIL type: scan amount={rec.get('amount')!r} is not float")
                all_ok = False

        # ── FINAL VERDICT ──
        print("\n" + "=" * 70)
        if all_ok:
            print("ALL CHECKS PASSED - fields consistent across all 3 endpoints")
        else:
            print("SOME CHECKS FAILED - see above")
        print("=" * 70)

    finally:
        await client.close()

asyncio.run(test())
