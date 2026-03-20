"""
Quick test: open and immediately close a minimal SHORT on Binance.
Uses the active account from trading_config.json.
"""
import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from config import BINANCE_ACCOUNTS
from binance_trader import BinanceTrader


async def main():
    # Read active account
    with open("trading_config.json") as f:
        cfg = json.load(f)
    account_name = cfg.get("active_account", "copytrading_privado")
    acct = BINANCE_ACCOUNTS[account_name]

    print(f"=== TEST LIVE TRADE ===")
    print(f"Account: {account_name} ({acct['label']})")

    trader = BinanceTrader(
        api_key=acct["api_key"],
        api_secret=acct["api_secret"],
        account_name=account_name,
    )
    await trader.connect()

    # 1. Check balance
    bal = await trader.get_account_balance()
    print(f"Balance: ${bal['balance']:.2f} | Available: ${bal['available']:.2f}")

    if bal["available"] < 5:
        print("❌ Not enough balance for test (need at least $5)")
        await trader.close()
        return

    # 2. Use DOGEUSDT with minimum notional
    symbol = "DOGEUSDT"
    leverage = 1

    # Get current price
    import httpx
    async with httpx.AsyncClient() as client:
        r = await client.get(f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}")
        price = float(r.json()["price"])
    print(f"DOGE price: ${price:.6f}")

    # Minimum notional ~ $6 to be safe (Binance min is usually $5)
    notional = 6.0
    qty = await trader.calc_quantity(symbol, notional, price)
    print(f"Test qty: {qty} BTC (notional ~${notional})")

    if qty <= 0:
        print("❌ Calculated qty is 0, notional too small")
        await trader.close()
        return

    # 3. OPEN SHORT
    print(f"\n>>> OPENING SHORT {symbol} qty={qty} lev={leverage}...")
    try:
        result = await trader.open_short(
            symbol=symbol,
            quantity=qty,
            leverage=leverage,
            # No TP/SL — we close immediately
        )
        order_id = result.get("orderId")
        avg_price = result.get("avgPrice", price)
        exec_qty = result.get("executedQty", qty)
        status = result.get("status")
        print(f"✅ OPEN: orderId={order_id} | status={status} | "
              f"qty={exec_qty} @ ${float(avg_price):,.2f}")
    except Exception as e:
        print(f"❌ OPEN FAILED: {e}")
        await trader.close()
        return

    # Small delay to let Binance process
    await asyncio.sleep(0.5)

    # 4. CLOSE IMMEDIATELY
    print(f"\n>>> CLOSING position {symbol}...")
    try:
        close_result = await trader.close_position(symbol=symbol)
        close_status = close_result.get("status")
        close_price = close_result.get("avgPrice", "?")
        print(f"✅ CLOSED: status={close_status} | "
              f"price=${float(close_price):,.2f}")
    except Exception as e:
        print(f"❌ CLOSE FAILED: {e}")
        print("⚠️ CHECK BINANCE MANUALLY — position may still be open!")

    # 5. Final balance
    await asyncio.sleep(0.5)
    bal2 = await trader.get_account_balance()
    diff = bal2["balance"] - bal["balance"]
    print(f"\nFinal balance: ${bal2['balance']:.2f} (diff: {'+' if diff >= 0 else ''}{diff:.4f})")
    print("=== TEST COMPLETE ===")

    await trader.close()


if __name__ == "__main__":
    asyncio.run(main())
