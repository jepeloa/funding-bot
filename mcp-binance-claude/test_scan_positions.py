"""Test scan_symbol_positions with a real symbol."""
import asyncio
import json
import sys
import time
sys.path.insert(0, "/home/javier/mcp_data_crypto/src")

from psi_jam_mcp.copytrading_client import CopyTradingClient

async def test():
    client = CopyTradingClient()
    try:
        # Test: Scan ETHUSDT positions across top 50 traders by AUM
        print("=" * 70)
        print("SCAN: ETHUSDT positions across top 50 traders (by AUM)")
        print("=" * 70)
        t0 = time.time()
        result = await client.scan_symbol_positions(
            symbol="ETHUSDT",
            top_n=50,
            sort_by="AUM",
        )
        elapsed = time.time() - t0
        print(f"Completed in {elapsed:.1f}s\n")

        s = result["summary"]
        print(f"Traders scanned: {result['scan_config']['top_n_traders_scanned']}")
        print(f"Traders with ETHUSDT position: {s['traders_with_position']}")
        print(f"")
        print(f"LONGS:  {s['long_count']} traders | ${s['total_long_notional_usdt']:,.2f} notional | AUM behind: ${s['long_aum_behind_usdt']:,.2f}")
        print(f"SHORTS: {s['short_count']} traders | ${s['total_short_notional_usdt']:,.2f} notional | AUM behind: ${s['short_aum_behind_usdt']:,.2f}")
        print(f"")
        print(f"Net bias: {s['net_bias']} ({s['long_pct']:.1f}% vs {s['short_pct']:.1f}%)")
        print(f"Bias ratio: {s['bias_ratio']}x")

        if result.get("longs"):
            print(f"\n--- LONG positions ({len(result['longs'])}) ---")
            for r in result["longs"]:
                print(f"  {r['nickname']:<20} | ${r['notional_usdt']:>12,.2f} | {r['leverage']}x | entry={r['entry_price']} | PnL=${r['unrealized_pnl_usdt']:>10,.2f} | AUM=${r['aum_usdt']:>12,.0f} | copiers={r['copiers']}")

        if result.get("shorts"):
            print(f"\n--- SHORT positions ({len(result['shorts'])}) ---")
            for r in result["shorts"]:
                print(f"  {r['nickname']:<20} | ${r['notional_usdt']:>12,.2f} | {r['leverage']}x | entry={r['entry_price']} | PnL=${r['unrealized_pnl_usdt']:>10,.2f} | AUM=${r['aum_usdt']:>12,.0f} | copiers={r['copiers']}")

        # Test 2: try BTCUSDT with top 100
        print("\n" + "=" * 70)
        print("SCAN: BTCUSDT positions across top 100 traders (by AUM)")
        print("=" * 70)
        t0 = time.time()
        result2 = await client.scan_symbol_positions(
            symbol="BTCUSDT",
            top_n=100,
            sort_by="AUM",
        )
        elapsed = time.time() - t0
        print(f"Completed in {elapsed:.1f}s\n")

        s2 = result2["summary"]
        print(f"Traders scanned: {result2['scan_config']['top_n_traders_scanned']}")
        print(f"Traders with BTCUSDT position: {s2['traders_with_position']}")
        print(f"LONGS:  {s2['long_count']} traders | ${s2['total_long_notional_usdt']:,.2f} notional")
        print(f"SHORTS: {s2['short_count']} traders | ${s2['total_short_notional_usdt']:,.2f} notional")
        print(f"Net bias: {s2['net_bias']} ({s2['long_pct']:.1f}% vs {s2['short_pct']:.1f}%) | ratio: {s2['bias_ratio']}x")

        if result2.get("longs"):
            print(f"\n--- Top 5 LONG ---")
            for r in result2["longs"][:5]:
                print(f"  {r['nickname']:<20} | ${r['notional_usdt']:>12,.2f} | {r['leverage']}x | AUM=${r['aum_usdt']:>12,.0f}")
        if result2.get("shorts"):
            print(f"\n--- Top 5 SHORT ---")
            for r in result2["shorts"][:5]:
                print(f"  {r['nickname']:<20} | ${r['notional_usdt']:>12,.2f} | {r['leverage']}x | AUM=${r['aum_usdt']:>12,.0f}")

    finally:
        await client.close()

asyncio.run(test())
