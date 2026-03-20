"""
Test: verify live sizing uses real Binance balance with correct percentage.
Simulates what _open_trade does for Base variant WITHOUT actually opening a trade.
"""
import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from config import BINANCE_ACCOUNTS, VARIANTS
from binance_trader import BinanceTrader


async def main():
    with open("trading_config.json") as f:
        cfg = json.load(f)
    account_name = cfg.get("active_account", "copytrading_privado")
    acct = BINANCE_ACCOUNTS[account_name]

    trader = BinanceTrader(
        api_key=acct["api_key"],
        api_secret=acct["api_secret"],
        account_name=account_name,
    )
    await trader.connect()

    # 1. Get real balance
    bal = await trader.get_account_balance()
    available = bal["available"]
    print(f"Account: {account_name}")
    print(f"Balance: ${bal['balance']:.2f}")
    print(f"Available: ${available:.2f}")
    print()

    # 2. Simulate sizing for each variant (as _open_trade would)
    print(f"{'Variant':<16} {'cap_frac':>8} {'Leverage':>8} {'Margin':>10} {'Notional':>10}")
    print("-" * 60)
    for vname, vparams in VARIANTS.items():
        cap_frac = vparams["capital_fraction"]
        leverage = vparams["leverage"]
        margin = cap_frac * available
        notional = margin * leverage
        print(f"{vname:<16} {cap_frac:>8.1%} {leverage:>7}x ${margin:>9.2f} ${notional:>9.2f}")

    # 3. Detailed check for Base (the live variant)
    base = VARIANTS["base"]
    cap_frac = base["capital_fraction"]
    leverage = base["leverage"]
    margin = cap_frac * available
    notional = margin * leverage

    print(f"\n=== BASE VARIANT (LIVE) ===")
    print(f"Formula: notional = capital_fraction × available_balance × leverage")
    print(f"         notional = {cap_frac} × ${available:.2f} × {leverage}")
    print(f"         notional = ${notional:.2f}")
    print(f"         margin   = ${margin:.2f} ({cap_frac:.0%} of ${available:.2f})")

    # 4. Verify with a real symbol
    symbol = "DOGEUSDT"
    import httpx
    async with httpx.AsyncClient() as client:
        r = await client.get(f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}")
        price = float(r.json()["price"])

    qty = await trader.calc_quantity(symbol, notional, price)
    print(f"\n  Example: {symbol} @ ${price:.6f}")
    print(f"  Quantity: {qty} DOGE")
    print(f"  Actual notional: ${qty * price:.2f}")

    # 5. Compare with old $10,000 paper sizing
    paper_notional = cap_frac * 10_000 * leverage
    print(f"\n  Paper sizing ($10k): ${paper_notional:.2f}")
    print(f"  Live sizing (real):  ${notional:.2f}")
    print(f"  Ratio: {notional/paper_notional:.1%} of paper size")

    await trader.close()


if __name__ == "__main__":
    asyncio.run(main())
