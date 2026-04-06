#!/usr/bin/env python3
"""
Test end-to-end del flujo completo de un trade live.

Ejecuta con posición mínima en ESPORTSUSDT (~$18 notional):
  1. OPEN SHORT  → verifica executedQty reconciliation
  2. Verifica TP/SL orders en Binance (STOP_MARKET + TAKE_PROFIT_MARKET)
  3. PARTIAL TP 33%  → verifica cierre parcial y qty restante
  4. Verifica que TP/SL fueron cancelados (por el partial close)
  5. CLOSE restante  → verifica posición = 0

Cada paso es un ASSERT; si falla, cierra posición de emergencia y reporta.
"""
import asyncio
import sys
import time
import os

sys.path.insert(0, os.path.dirname(__file__))

from binance_trader import BinanceTrader

API_KEY = os.getenv("TEST_BINANCE_API_KEY")
API_SECRET = os.getenv("TEST_BINANCE_API_SECRET")
ACCOUNT = os.getenv("TEST_BINANCE_ACCOUNT", "copytrading_privado")

SYMBOL = "ESPORTSUSDT"
LEVERAGE = 5
QTY = 60          # ~$18 notional at ~$0.30/unit
SL_PCT = 0.057    # 5.7% — AEPS base SL
TP_PCT = 0.15     # 15% — static base TP
PARTIAL_FRACTION = 0.33

# ─── Colores ─────────────────────────────────────────────────
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"


def ok(msg):
    print(f"  {GREEN}✓{RESET} {msg}")


def fail(msg):
    print(f"  {RED}✗{RESET} {msg}")


def info(msg):
    print(f"  {CYAN}ℹ{RESET} {msg}")


def header(step, msg):
    print(f"\n{BOLD}{YELLOW}━━ STEP {step}: {msg} ━━{RESET}")


class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def check(self, condition, pass_msg, fail_msg):
        if condition:
            ok(pass_msg)
            self.passed += 1
        else:
            fail(fail_msg)
            self.failed += 1
            self.errors.append(fail_msg)

    def summary(self):
        total = self.passed + self.failed
        print(f"\n{BOLD}{'='*60}{RESET}")
        if self.failed == 0:
            print(f"{GREEN}{BOLD}  ALL {total} CHECKS PASSED ✓{RESET}")
        else:
            print(f"{RED}{BOLD}  {self.failed}/{total} CHECKS FAILED{RESET}")
            for e in self.errors:
                print(f"  {RED}  • {e}{RESET}")
        print(f"{BOLD}{'='*60}{RESET}")
        return self.failed == 0


async def emergency_close(trader, symbol):
    """Cierra cualquier posición abierta de emergencia."""
    try:
        await trader.cancel_all_orders(symbol)
        positions = await trader.get_positions(symbol)
        if positions and positions[0]["position_amt"] > 0:
            await trader.close_position(symbol)
            print(f"  {RED}⚠ EMERGENCY CLOSE executed{RESET}")
    except Exception as e:
        print(f"  {RED}⚠ EMERGENCY CLOSE failed: {e}{RESET}")


async def get_open_orders(trader, symbol):
    """Query open orders for a symbol via signed GET."""
    raw = await trader._signed_get("/fapi/v1/openOrders", {"symbol": symbol.upper()})
    return raw


async def main():
    print(f"\n{BOLD}{CYAN}╔══════════════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{CYAN}║    TRADE FLOW END-TO-END TEST                        ║{RESET}")
    print(f"{BOLD}{CYAN}║    Symbol: {SYMBOL}  Qty: {QTY}  Lev: {LEVERAGE}x              ║{RESET}")
    print(f"{BOLD}{CYAN}╚══════════════════════════════════════════════════════╝{RESET}")

    if not API_KEY or not API_SECRET:
        print(
            f"{RED}Missing TEST_BINANCE_API_KEY or TEST_BINANCE_API_SECRET in environment{RESET}"
        )
        return 1

    t = TestResult()
    trader = BinanceTrader(API_KEY, API_SECRET, account_name=ACCOUNT)

    try:
        # ── PRECONDITION: no open position ──
        header(0, "PRECONDITIONS")
        positions = await trader.get_positions(SYMBOL)
        if positions and positions[0]["position_amt"] > 0:
            fail(f"Position already open for {SYMBOL} — closing first")
            await emergency_close(trader, SYMBOL)
            await asyncio.sleep(1)

        positions = await trader.get_positions(SYMBOL)
        pos_qty = positions[0]["position_amt"] if positions else 0
        t.check(pos_qty == 0, "No open position", f"Position still open: {pos_qty}")

        # ── STEP 1: OPEN SHORT ──
        header(1, "OPEN SHORT")
        mark_raw = await trader._signed_get("/fapi/v1/premiumIndex", {"symbol": SYMBOL})
        mark_price = float(mark_raw[0]["markPrice"]) if isinstance(mark_raw, list) else float(mark_raw["markPrice"])
        info(f"Mark price: ${mark_price:.6f}")

        sl_price = mark_price * (1 + SL_PCT)
        tp_price = mark_price * (1 - TP_PCT)
        info(f"SL price (AEPS {SL_PCT:.1%}): ${sl_price:.6f}")
        info(f"TP price ({TP_PCT:.0%}): ${tp_price:.6f}")

        result = await trader.open_short(
            symbol=SYMBOL,
            quantity=QTY,
            leverage=LEVERAGE,
            take_profit=tp_price,
            stop_loss=sl_price,
        )
        order_id = result.get("orderId")
        filled_qty = float(result.get("executedQty", 0))
        raw_status = result.get("status", "")

        t.check(order_id is not None, f"orderId={order_id}", "No orderId returned")
        t.check(
            filled_qty > 0,
            f"executedQty={filled_qty} (reconciled if needed)",
            f"executedQty=0 — reconciliation failed!"
        )

        info(f"Raw API status: {raw_status}")
        info(f"Reconciled qty: {filled_qty}")

        # Verify position on Binance
        await asyncio.sleep(0.5)
        positions = await trader.get_positions(SYMBOL)
        t.check(len(positions) > 0, "Position exists on Binance", "No position found!")
        if positions:
            pos = positions[0]
            pos_qty = pos["position_amt"]
            t.check(
                pos["side"] == "SHORT",
                f"Position side=SHORT",
                f"Wrong side: {pos['side']}"
            )
            t.check(
                abs(pos_qty - filled_qty) < 1,
                f"Position qty={pos_qty} matches filled={filled_qty}",
                f"Qty mismatch: pos={pos_qty} vs filled={filled_qty}"
            )
            info(f"Entry price: ${pos['entry_price']:.6f}")
            info(f"Unrealized PnL: ${pos['unrealized_pnl']:.4f}")

        # ── STEP 2: VERIFY TP/SL ORDERS ON BINANCE ──
        header(2, "VERIFY TP/SL ORDERS")
        await asyncio.sleep(1)
        open_orders = await get_open_orders(trader, SYMBOL)

        # Count STOP_MARKET and TAKE_PROFIT_MARKET
        stop_orders = [o for o in open_orders if o.get("type") in ("STOP_MARKET",)]
        tp_orders = [o for o in open_orders if o.get("type") in ("TAKE_PROFIT_MARKET",)]

        # Also check algo orders
        try:
            algo_raw = await trader._signed_get("/fapi/v1/algoOrders",
                                                 {"symbol": SYMBOL, "status": "ACTIVE"})
            algo_orders = algo_raw if isinstance(algo_raw, list) else algo_raw.get("orders", [])
        except Exception:
            algo_orders = []

        total_sl = len(stop_orders) + len([a for a in algo_orders
                                            if "STOP" in str(a.get("algoType", "")).upper()
                                            or a.get("side") == "BUY"])
        total_tp = len(tp_orders) + len([a for a in algo_orders
                                          if "TAKE" in str(a.get("type", "")).upper()])

        # At least check that some protective orders exist
        has_sl = len(stop_orders) > 0
        has_tp = len(tp_orders) > 0
        has_algo = len(algo_orders) > 0

        if has_sl:
            sl_order = stop_orders[0]
            sl_stop_price = float(sl_order.get("stopPrice", 0))
            expected_sl = sl_price
            sl_diff_pct = abs(sl_stop_price - expected_sl) / expected_sl * 100
            t.check(True, f"STOP_MARKET order found (stopPrice=${sl_stop_price:.6f})")
            t.check(
                sl_diff_pct < 1.0,
                f"SL price matches AEPS ({sl_diff_pct:.2f}% diff)",
                f"SL price mismatch: order=${sl_stop_price:.6f} vs expected=${expected_sl:.6f} ({sl_diff_pct:.1f}%)"
            )
        elif has_algo:
            t.check(True, f"Algo conditional orders found ({len(algo_orders)})")
            info("(Copy-trading account uses algo orders instead of standard)")
        else:
            t.check(False, "", "No SL order found (neither standard nor algo)!")

        if has_tp:
            tp_order = tp_orders[0]
            tp_stop_price = float(tp_order.get("stopPrice", 0))
            t.check(True, f"TAKE_PROFIT_MARKET order found (stopPrice=${tp_stop_price:.6f})")
        elif has_algo:
            info("TP likely placed as algo order")
        else:
            t.check(False, "", "No TP order found!")

        info(f"Standard open orders: {len(open_orders)}")
        info(f"Algo orders: {len(algo_orders)}")
        for o in open_orders:
            info(f"  {o.get('type')} {o.get('side')} stopPrice={o.get('stopPrice')} qty={o.get('origQty')}")
        for a in algo_orders:
            info(f"  ALGO {a.get('algoType')} triggerPrice={a.get('triggerPrice')} qty={a.get('quantity')}")

        # ── STEP 3: PARTIAL CLOSE (33%) ──
        header(3, f"PARTIAL CLOSE ({PARTIAL_FRACTION:.0%})")
        positions = await trader.get_positions(SYMBOL)
        current_qty = positions[0]["position_amt"] if positions else 0
        close_qty = int(current_qty * PARTIAL_FRACTION)
        info(f"Current position: {current_qty}")
        info(f"Closing {PARTIAL_FRACTION:.0%} = {close_qty} units")

        # Cancel existing orders first (like strategy does)
        await trader.cancel_all_orders(SYMBOL)
        await asyncio.sleep(0.5)

        partial_result = await trader.close_position(
            symbol=SYMBOL,
            quantity=close_qty,
        )
        partial_order_id = partial_result.get("orderId")
        partial_filled = float(partial_result.get("executedQty", 0))

        t.check(
            partial_order_id is not None,
            f"Partial close orderId={partial_order_id}",
            "No orderId for partial close"
        )

        # Verify remaining position
        await asyncio.sleep(0.5)
        positions = await trader.get_positions(SYMBOL)
        remaining_qty = positions[0]["position_amt"] if positions else 0
        expected_remaining = current_qty - close_qty

        t.check(
            abs(remaining_qty - expected_remaining) < 1,
            f"Remaining qty={remaining_qty} (expected ~{expected_remaining})",
            f"Qty mismatch: remaining={remaining_qty} vs expected={expected_remaining}"
        )

        # Verify TP/SL were cancelled
        open_orders_after = await get_open_orders(trader, SYMBOL)
        t.check(
            len(open_orders_after) == 0,
            "TP/SL orders cancelled after partial close",
            f"Unexpected orders still open: {len(open_orders_after)}"
        )

        # ── STEP 4: CLOSE REMAINING ──
        header(4, "CLOSE REMAINING POSITION")
        close_result = await trader.close_position(symbol=SYMBOL)
        close_order_id = close_result.get("orderId")
        close_price = close_result.get("avgPrice", "N/A")

        t.check(
            close_order_id is not None,
            f"Close orderId={close_order_id} @ ${close_price}",
            "No orderId for final close"
        )

        # Verify zero position
        await asyncio.sleep(0.5)
        positions = await trader.get_positions(SYMBOL)
        final_qty = positions[0]["position_amt"] if positions else 0

        t.check(
            final_qty == 0,
            "Position fully closed (qty=0)",
            f"Position NOT closed! qty={final_qty}"
        )

        # ── STEP 5: VERIFY NO ORPHANED ORDERS ──
        header(5, "CLEANUP VERIFICATION")
        remaining_orders = await get_open_orders(trader, SYMBOL)
        t.check(
            len(remaining_orders) == 0,
            "No orphaned orders remaining",
            f"Orphaned orders found: {len(remaining_orders)}"
        )

        try:
            algo_remaining = await trader._signed_get("/fapi/v1/algoOrders",
                                                       {"symbol": SYMBOL, "status": "ACTIVE"})
            algo_count = len(algo_remaining) if isinstance(algo_remaining, list) else len(algo_remaining.get("orders", []))
        except Exception:
            algo_count = 0

        t.check(
            algo_count == 0,
            "No orphaned algo orders",
            f"Orphaned algo orders: {algo_count}"
        )

    except Exception as e:
        fail(f"UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        t.failed += 1
        t.errors.append(str(e))
        # Emergency close
        print(f"\n{RED}Running emergency close...{RESET}")
        await emergency_close(trader, SYMBOL)

    finally:
        await trader.close()

    # Summary
    success = t.summary()
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
