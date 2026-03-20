"""Quick test of the CopyTradingClient."""
import asyncio
import json
import sys
sys.path.insert(0, "/home/javier/mcp_data_crypto/src")

from psi_jam_mcp.copytrading_client import CopyTradingClient

async def test():
    client = CopyTradingClient()

    try:
        # Test 1: Search top traders by ROI, 30D
        print("=" * 70)
        print("TEST 1: search_top_traders (ROI, 30D, page 1, size 5)")
        print("=" * 70)
        result = await client.search_top_traders(
            page_number=1,
            page_size=5,
            time_range="30D",
            sort_by="ROI",
            order="DESC",
        )
        print(f"Total traders: {result.get('total', 'N/A')}")
        print(f"Traders returned: {len(result.get('traders', []))}")
        if result.get("daily_picks"):
            print(f"Daily picks: {len(result['daily_picks'])}")
        for i, t in enumerate(result.get("traders", [])[:5]):
            print(f"\n  #{i+1} {t['nickname']}")
            print(f"     ROI: {t['roi_pct']:.2f}%  |  PnL: ${t['pnl_usdt']:.2f}")
            print(f"     Win Rate: {t['win_rate_pct']}%  |  MDD: {t['mdd_pct']}%")
            print(f"     AUM: ${t['aum_usdt']:.2f}  |  Copiers: {t['copiers']}/{t['max_copiers']}")
            print(f"     Slots: {t['slots_available']}  |  Badge: {t['badge']}")
            print(f"     Portfolio ID: {t['portfolio_id']}")

        # Test 2: Get detail for the trader from the original URL
        print("\n" + "=" * 70)
        print("TEST 2: get_trader_detail (portfolio_id=4878630112238695169)")
        print("=" * 70)
        detail = await client.get_trader_detail(
            portfolio_id="4878630112238695169",
            time_range="7D",
        )
        if detail.get("profile"):
            p = detail["profile"]
            s = detail.get("stats", {})
            print(f"  Nombre: {p['nickname']} ({p.get('nickname_translate', '')})")
            print(f"  Estado: {p['status']}  |  Badge: {p.get('badge')}")
            print(f"  Balance: ${s.get('margin_balance_usdt')} USDT")
            print(f"  AUM: ${s.get('aum_usdt')} USDT")
            print(f"  Copier PnL: ${s.get('copier_pnl_usdt')} USDT")
            print(f"  Win Rate: {s.get('win_rate')}%")
            print(f"  Copiers: {s.get('current_copiers')}/{s.get('max_copiers')}")
            print(f"  Profit Sharing: {s.get('profit_sharing_pct')}%")

        if detail.get("roi_chart"):
            print(f"\n  ROI Chart ({len(detail['roi_chart'])} points):")
            for c in detail["roi_chart"]:
                print(f"    {c['roi_pct']:.2f}%")

        if detail.get("coin_distribution"):
            print(f"\n  Distribucion por moneda:")
            for c in detail["coin_distribution"]:
                print(f"    {c['asset']}: {c['volume']}%")

        if detail.get("open_positions"):
            print(f"\n  Posiciones abiertas: {len(detail['open_positions'])}")
            for pos in detail["open_positions"][:5]:
                print(f"    {pos['symbol']} {pos['side']} x{pos['leverage']} | PnL: {pos['unrealized_pnl']}")
        else:
            print(f"\n  Sin posiciones abiertas activas")
            print(f"  Symbols configurados: {detail.get('total_position_symbols', 0)}")

        # Test 3: Search by nickname
        print("\n" + "=" * 70)
        print("TEST 3: search_top_traders (nickname search)")
        print("=" * 70)
        result3 = await client.search_top_traders(
            nickname="Money",
            page_size=5,
        )
        print(f"Results for 'Money': {result3.get('total', 0)} traders")
        for t in result3.get("traders", [])[:3]:
            print(f"  {t['nickname']} | ROI: {t['roi_pct']}% | ID: {t['portfolio_id']}")

    finally:
        await client.close()

asyncio.run(test())
