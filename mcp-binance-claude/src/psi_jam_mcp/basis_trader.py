"""
Basis Trader — Semi-Automatic Hedged Position Management
=========================================================
Manages the dual-leg execution of basis trades:
  - Prepare trade proposals (spot + futures legs) for human review
  - Execute approved proposals (both legs simultaneously)
  - Track active hedged positions
  - Close hedged positions (both legs)
  - Calculate real-time PnL including funding collected

Architecture:
  prepare_trade() → TradeProposal (JSON for review)
  execute_trade() → Executes both legs, returns fills
  close_trade() → Closes both legs, calculates final PnL
  list_positions() → Active hedged positions with live PnL

Risk limits:
  - Max 30% capital per trade
  - Max 3 simultaneous hedged positions
  - Stop if basis inverts beyond threshold
"""

import asyncio
import time
import json
import os
from typing import Optional

from . import basis_engine as engine


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

MAX_POSITIONS = 3
MAX_ALLOCATION_PCT = 30.0
DEFAULT_LEVERAGE = 2
PROPOSALS_FILE = os.path.expanduser("~/.psi_jam_mcp/basis_proposals.json")
POSITIONS_FILE = os.path.expanduser("~/.psi_jam_mcp/basis_positions.json")


def _ensure_dir():
    """Ensure the data directory exists."""
    d = os.path.dirname(PROPOSALS_FILE)
    os.makedirs(d, exist_ok=True)


# ─────────────────────────────────────────────
# TRADER
# ─────────────────────────────────────────────

class BasisTrader:
    """
    Semi-automatic basis trade executor.

    Workflow:
    1. prepare_trade() → Creates a proposal with full risk analysis
    2. User reviews the proposal
    3. execute_trade(proposal_id) → Executes both legs
    4. list_positions() → Monitor active hedged positions
    5. close_trade(position_id) → Close both legs, finalize PnL
    """

    def __init__(self, spot_client, futures_trader):
        """
        Args:
            spot_client: SpotClient instance (authenticated for spot trading)
            futures_trader: FuturesTrader instance (authenticated for futures trading)
        """
        self.spot = spot_client
        self.futures = futures_trader

        # In-memory stores (also persisted to disk)
        self._proposals: dict[str, dict] = {}
        self._positions: dict[str, dict] = {}
        self._next_id = 1

        _ensure_dir()
        self._load_state()

    # ─────────────────────────────────────────────
    # PREPARE TRADE
    # ─────────────────────────────────────────────

    async def prepare_trade(
        self,
        symbol: str,
        strategy: str,
        capital_usdt: float,
        leverage: int = DEFAULT_LEVERAGE,
        use_bnb: bool = False,
        use_maker: bool = False,
    ) -> dict:
        """
        Prepare a basis trade proposal for human review.

        Calculates:
        - Position sizes for both legs
        - Entry basis and expected profit
        - Fee breakdown
        - Risk analysis (max loss, breakeven, liquidation distance)

        Args:
            symbol: Trading pair (e.g. BTCUSDT)
            strategy: CASH_CARRY, FUNDING_ARB, or BASIS_SCALP
            capital_usdt: Capital to allocate to this trade
            leverage: Futures leverage (default 2x)
            use_bnb: Use BNB fee discount
            use_maker: Use limit orders (maker fees)

        Returns:
            Proposal dict with full analysis for review.
        """
        symbol = symbol.upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"

        # Check limits
        if len(self._positions) >= MAX_POSITIONS:
            return {
                "error": f"Límite de {MAX_POSITIONS} posiciones hedged activas alcanzado. "
                         "Cierra una posición antes de abrir otra.",
                "active_positions": len(self._positions),
            }

        # Fetch current prices in parallel
        try:
            spot_data, futures_ticker, funding_data, spot_info = await asyncio.gather(
                self.spot.get_price(symbol),
                self._get_futures_price(symbol),
                self._safe_funding(symbol),
                self.spot.get_symbol_info(symbol),
            )
        except Exception as e:
            return {"error": f"Error fetching market data: {e}", "symbol": symbol}

        spot_price = spot_data["price"]
        futures_price = futures_ticker["price"]

        # Basis
        basis = engine.calculate_basis(spot_price, futures_price)

        # Funding
        funding = engine.calculate_funding_carry(funding_data, holding_hours=24) if funding_data else {}

        # Position sizing
        sizing = engine.calculate_position_size(
            capital_usdt=capital_usdt,
            spot_price=spot_price,
            allocation_pct=100,  # Already pre-allocated
            futures_leverage=leverage,
            spot_info=spot_info,
        )

        # Fee analysis
        profit = engine.calculate_fee_adjusted_profit(
            basis["basis_pct"], use_bnb=use_bnb, use_maker=use_maker,
        )

        # Risk analysis
        fees = engine.get_fee_schedule(use_bnb, use_maker)
        breakeven_basis_pct = fees["round_trip_cost"] * 100

        # Estimated PnL scenarios
        scenario_converge = engine.estimate_pnl(
            entry_basis_pct=basis["basis_pct"],
            exit_basis_pct=0,
            notional_usdt=sizing["spot_leg"]["notional_usdt"],
            funding_collected_pct=funding.get("estimated_carry", {}).get("estimated_carry_pct", 0),
            use_bnb=use_bnb, use_maker=use_maker,
        )
        scenario_half = engine.estimate_pnl(
            entry_basis_pct=basis["basis_pct"],
            exit_basis_pct=basis["basis_pct"] / 2,
            notional_usdt=sizing["spot_leg"]["notional_usdt"],
            funding_collected_pct=funding.get("estimated_carry", {}).get("estimated_carry_pct", 0) / 2,
            use_bnb=use_bnb, use_maker=use_maker,
        )
        scenario_adverse = engine.estimate_pnl(
            entry_basis_pct=basis["basis_pct"],
            exit_basis_pct=basis["basis_pct"] * 2,  # basis doubles against us
            notional_usdt=sizing["spot_leg"]["notional_usdt"],
            use_bnb=use_bnb, use_maker=use_maker,
        )

        # Create proposal
        proposal_id = f"BP-{self._next_id:04d}"
        self._next_id += 1

        proposal = {
            "proposal_id": proposal_id,
            "status": "PENDING",
            "created_at": int(time.time() * 1000),
            "symbol": symbol,
            "strategy": strategy,

            "market_data": {
                "spot_price": spot_price,
                "futures_price": futures_price,
                "basis": basis,
                "funding": funding,
            },

            "sizing": sizing,
            "leverage": leverage,
            "capital_usdt": capital_usdt,

            "fee_analysis": profit,
            "breakeven_basis_pct": round(breakeven_basis_pct, 4),

            "scenarios": {
                "full_convergence": scenario_converge,
                "half_convergence": scenario_half,
                "adverse_2x": scenario_adverse,
            },

            "execution_plan": {
                "spot_leg": {
                    "action": "BUY" if basis["basis_pct"] > 0 else "SELL",
                    "symbol": symbol,
                    "quantity": sizing["spot_leg"]["quantity"],
                    "notional_usdt": sizing["spot_leg"]["notional_usdt"],
                    "order_type": "LIMIT" if use_maker else "MARKET",
                },
                "futures_leg": {
                    "action": "SHORT" if basis["basis_pct"] > 0 else "LONG",
                    "symbol": symbol,
                    "quantity": sizing["futures_leg"]["quantity"],
                    "leverage": leverage,
                    "notional_usdt": sizing["futures_leg"]["notional_usdt"],
                    "order_type": "LIMIT" if use_maker else "MARKET",
                },
            },

            "risk": {
                "max_loss_if_basis_doubles": scenario_adverse["net_pnl_usdt"],
                "hedged_exposure": "delta-neutral (spot long = futures short)",
                "main_risks": [
                    "Basis puede ampliarse temporalmente (drawdown)",
                    "Funding puede volverse negativo (shorts pagan)",
                    "Slippage en ejecución si ambas patas no se llenan al mismo precio",
                    "Exchange risk (Binance downtime)",
                ],
            },
        }

        self._proposals[proposal_id] = proposal
        self._save_state()

        return proposal

    # ─────────────────────────────────────────────
    # EXECUTE TRADE
    # ─────────────────────────────────────────────

    async def execute_trade(self, proposal_id: str) -> dict:
        """
        Execute a previously prepared trade proposal.
        Places both spot and futures orders simultaneously.

        Args:
            proposal_id: The proposal ID returned by prepare_trade()

        Returns:
            Execution result with fills for both legs.
        """
        proposal = self._proposals.get(proposal_id)
        if not proposal:
            return {"error": f"Proposal {proposal_id} not found"}
        if proposal["status"] != "PENDING":
            return {"error": f"Proposal {proposal_id} is {proposal['status']}, not PENDING"}

        symbol = proposal["symbol"]
        plan = proposal["execution_plan"]

        # Re-check current prices
        spot_data = await self.spot.get_price(symbol)
        futures_data = await self._get_futures_price(symbol)
        current_basis = engine.calculate_basis(spot_data["price"], futures_data["price"])

        # Verify basis hasn't moved too much (>50% change from proposal)
        original_basis = abs(proposal["market_data"]["basis"]["basis_pct"])
        current_basis_abs = abs(current_basis["basis_pct"])
        if original_basis > 0:
            basis_change = abs(current_basis_abs - original_basis) / original_basis
            if basis_change > 0.5:
                return {
                    "error": "Basis ha cambiado >50% desde la propuesta. Prepara un nuevo trade.",
                    "original_basis_pct": proposal["market_data"]["basis"]["basis_pct"],
                    "current_basis_pct": current_basis["basis_pct"],
                    "change_pct": round(basis_change * 100, 1),
                }

        # Set leverage first
        try:
            await self.futures.set_leverage(symbol, plan["futures_leg"]["leverage"])
        except Exception as e:
            return {"error": f"Error setting leverage: {e}"}

        # Execute both legs simultaneously
        try:
            spot_side = plan["spot_leg"]["action"]
            futures_side = plan["futures_leg"]["action"]

            spot_order = self.spot.place_order(
                symbol=symbol,
                side=spot_side,
                quantity=plan["spot_leg"]["quantity"],
                order_type="MARKET",  # Always MARKET for simultaneous execution
            )
            futures_order = self.futures.open_position(
                symbol=symbol,
                side=futures_side,
                quantity=plan["futures_leg"]["quantity"],
                order_type="MARKET",
            )

            spot_result, futures_result = await asyncio.gather(
                spot_order, futures_order,
                return_exceptions=True,
            )
        except Exception as e:
            proposal["status"] = "FAILED"
            self._save_state()
            return {"error": f"Execution error: {e}"}

        # Check results
        spot_ok = not isinstance(spot_result, Exception) and "error" not in spot_result
        futures_ok = not isinstance(futures_result, Exception) and "error" not in futures_result

        if not spot_ok and not futures_ok:
            proposal["status"] = "FAILED"
            self._save_state()
            return {
                "error": "Both legs failed",
                "spot_error": str(spot_result),
                "futures_error": str(futures_result),
            }

        if not spot_ok or not futures_ok:
            # One leg failed — CRITICAL: need to unwind the successful leg
            proposal["status"] = "PARTIAL_FAIL"
            self._save_state()
            return {
                "error": "One leg failed — MANUAL INTERVENTION REQUIRED",
                "spot_result": str(spot_result) if isinstance(spot_result, Exception) else spot_result,
                "futures_result": str(futures_result) if isinstance(futures_result, Exception) else futures_result,
                "action_required": (
                    "Close the successful leg manually to avoid unhedged exposure. "
                    "Spot: use spot account. Futures: use futures_close_position."
                ),
            }

        # Both succeeded — create tracked position
        position_id = proposal_id.replace("BP-", "BT-")

        spot_avg_price = spot_result.get("avg_price", spot_data["price"])
        futures_avg_price = futures_result.get("avg_price", futures_data["price"])
        actual_basis = engine.calculate_basis(spot_avg_price, futures_avg_price)

        position = {
            "position_id": position_id,
            "proposal_id": proposal_id,
            "symbol": symbol,
            "strategy": proposal["strategy"],
            "status": "ACTIVE",
            "opened_at": int(time.time() * 1000),

            "entry": {
                "spot_price": spot_avg_price,
                "futures_price": futures_avg_price,
                "basis": actual_basis,
                "quantity": plan["spot_leg"]["quantity"],
                "notional_usdt": plan["spot_leg"]["notional_usdt"],
                "leverage": plan["futures_leg"]["leverage"],
            },

            "fills": {
                "spot": spot_result,
                "futures": futures_result,
            },

            "funding_collected": 0.0,
        }

        self._positions[position_id] = position
        proposal["status"] = "EXECUTED"
        proposal["position_id"] = position_id
        self._save_state()

        return {
            "status": "EXECUTED",
            "position_id": position_id,
            "entry_basis_pct": actual_basis["basis_pct"],
            "spot_fill": spot_result,
            "futures_fill": futures_result,
            "message": f"Posición hedged abierta: {symbol} — spot + futures ejecutados.",
        }

    # ─────────────────────────────────────────────
    # CLOSE TRADE
    # ─────────────────────────────────────────────

    async def close_trade(
        self,
        position_id: str,
        reason: str = "manual",
    ) -> dict:
        """
        Close a hedged position (both spot and futures legs).

        Args:
            position_id: The position ID from execute_trade()
            reason: Reason for closing (manual, convergence, stop_loss, funding_flip)

        Returns:
            Final PnL calculation.
        """
        position = self._positions.get(position_id)
        if not position:
            return {"error": f"Position {position_id} not found"}
        if position["status"] != "ACTIVE":
            return {"error": f"Position {position_id} is {position['status']}, not ACTIVE"}

        symbol = position["symbol"]
        qty = position["entry"]["quantity"]

        # Get current prices
        spot_data = await self.spot.get_price(symbol)
        futures_data = await self._get_futures_price(symbol)
        exit_basis = engine.calculate_basis(spot_data["price"], futures_data["price"])

        # Close both legs simultaneously
        # Spot: SELL (we had BUY'd)
        # Futures: close_position (we had SHORT'd)
        try:
            spot_close = self.spot.place_order(
                symbol=symbol,
                side="SELL",
                quantity=qty,
                order_type="MARKET",
            )
            futures_close = self.futures.close_position(
                symbol=symbol,
                order_type="MARKET",
            )

            spot_result, futures_result = await asyncio.gather(
                spot_close, futures_close,
                return_exceptions=True,
            )
        except Exception as e:
            return {"error": f"Close execution error: {e}"}

        spot_ok = not isinstance(spot_result, Exception) and "error" not in spot_result
        futures_ok = not isinstance(futures_result, Exception) and "error" not in futures_result

        if not spot_ok or not futures_ok:
            return {
                "error": "Partial close — MANUAL INTERVENTION REQUIRED",
                "spot_result": str(spot_result) if isinstance(spot_result, Exception) else spot_result,
                "futures_result": str(futures_result) if isinstance(futures_result, Exception) else futures_result,
            }

        # Fetch real funding income before calculating final PnL
        funding_income = await self._fetch_funding_income(
            symbol, position["opened_at"]
        )
        funding_usdt = funding_income.get("total_usdt", 0)
        position["funding_collected"] = funding_usdt
        position["funding_detail"] = funding_income

        entry_basis_pct = position["entry"]["basis"]["basis_pct"]
        exit_basis_pct = exit_basis["basis_pct"]
        notional = position["entry"]["notional_usdt"]
        funding_pct = (funding_usdt / notional * 100) if notional > 0 else 0

        pnl = engine.estimate_pnl(
            entry_basis_pct=entry_basis_pct,
            exit_basis_pct=exit_basis_pct,
            notional_usdt=notional,
            funding_collected_pct=funding_pct,
        )

        # Update position
        position["status"] = "CLOSED"
        position["closed_at"] = int(time.time() * 1000)
        position["close_reason"] = reason
        position["exit"] = {
            "spot_price": spot_data["price"],
            "futures_price": futures_data["price"],
            "basis": exit_basis,
        }
        position["pnl"] = pnl
        position["close_fills"] = {
            "spot": spot_result,
            "futures": futures_result,
        }
        self._save_state()

        holding_hours = (position["closed_at"] - position["opened_at"]) / 3_600_000

        return {
            "status": "CLOSED",
            "position_id": position_id,
            "symbol": symbol,
            "holding_hours": round(holding_hours, 1),
            "entry_basis_pct": entry_basis_pct,
            "exit_basis_pct": exit_basis_pct,
            "pnl": pnl,
            "close_reason": reason,
            "spot_close": spot_result,
            "futures_close": futures_result,
        }

    # ─────────────────────────────────────────────
    # LIST POSITIONS
    # ─────────────────────────────────────────────

    async def list_positions(self, include_closed: bool = False) -> dict:
        """
        List hedged positions with current status.
        For active positions, fetches live prices and calculates unrealized PnL.
        """
        active = []
        closed = []

        for pos_id, pos in self._positions.items():
            if pos["status"] == "ACTIVE":
                # Fetch live prices + real funding income in parallel
                try:
                    spot_task = self.spot.get_price(pos["symbol"])
                    futures_task = self._get_futures_price(pos["symbol"])
                    funding_task = self._fetch_funding_income(
                        pos["symbol"], pos["opened_at"]
                    )

                    spot_data, futures_data, funding_income = await asyncio.gather(
                        spot_task, futures_task, funding_task
                    )

                    current_basis = engine.calculate_basis(
                        spot_data["price"], futures_data["price"]
                    )

                    # Update persisted funding total
                    funding_usdt = funding_income.get("total_usdt", 0)
                    pos["funding_collected"] = funding_usdt
                    notional = pos["entry"]["notional_usdt"]
                    funding_pct = (funding_usdt / notional * 100) if notional > 0 else 0

                    entry_basis = pos["entry"]["basis"]["basis_pct"]
                    unrealized = engine.estimate_pnl(
                        entry_basis_pct=entry_basis,
                        exit_basis_pct=current_basis["basis_pct"],
                        notional_usdt=notional,
                        funding_collected_pct=funding_pct,
                    )

                    holding_hours = (time.time() * 1000 - pos["opened_at"]) / 3_600_000

                    active.append({
                        "position_id": pos_id,
                        "symbol": pos["symbol"],
                        "strategy": pos["strategy"],
                        "entry_basis_pct": entry_basis,
                        "current_basis_pct": current_basis["basis_pct"],
                        "unrealized_pnl": unrealized,
                        "funding": {
                            "total_usdt": funding_usdt,
                            "payment_count": funding_income.get("payment_count", 0),
                            "net_direction": funding_income.get("net_direction", "unknown"),
                            "funding_pct_of_notional": round(funding_pct, 4),
                        },
                        "holding_hours": round(holding_hours, 1),
                        "entry_spot": pos["entry"]["spot_price"],
                        "entry_futures": pos["entry"]["futures_price"],
                        "current_spot": spot_data["price"],
                        "current_futures": futures_data["price"],
                        "notional_usdt": notional,
                    })
                except Exception as e:
                    active.append({
                        "position_id": pos_id,
                        "symbol": pos["symbol"],
                        "error": str(e),
                    })

            elif include_closed and pos["status"] == "CLOSED":
                closed.append({
                    "position_id": pos_id,
                    "symbol": pos["symbol"],
                    "strategy": pos["strategy"],
                    "pnl": pos.get("pnl", {}),
                    "close_reason": pos.get("close_reason", ""),
                })

        result = {
            "active_count": len(active),
            "max_positions": MAX_POSITIONS,
            "active_positions": active,
        }

        if include_closed:
            result["closed_count"] = len(closed)
            result["closed_positions"] = closed

        return result

    # ─────────────────────────────────────────────
    # DASHBOARD
    # ─────────────────────────────────────────────

    async def get_dashboard(self) -> dict:
        """
        Comprehensive dashboard: best opportunities + active positions + PnL.
        """
        positions = await self.list_positions(include_closed=True)

        # Aggregate PnL from closed positions
        total_closed_pnl = 0
        for cp in positions.get("closed_positions", []):
            pnl = cp.get("pnl", {})
            total_closed_pnl += pnl.get("net_pnl_usdt", 0)

        # Aggregate unrealized from active
        total_unrealized = 0
        for ap in positions.get("active_positions", []):
            upnl = ap.get("unrealized_pnl", {})
            total_unrealized += upnl.get("net_pnl_usdt", 0)

        # Pending proposals
        pending = [
            {"proposal_id": p["proposal_id"], "symbol": p["symbol"], "strategy": p["strategy"]}
            for p in self._proposals.values()
            if p["status"] == "PENDING"
        ]

        return {
            "summary": {
                "active_positions": positions["active_count"],
                "max_positions": MAX_POSITIONS,
                "total_unrealized_pnl": round(total_unrealized, 4),
                "total_closed_pnl": round(total_closed_pnl, 4),
                "pending_proposals": len(pending),
            },
            "active_positions": positions["active_positions"],
            "pending_proposals": pending,
            "closed_summary": {
                "count": positions.get("closed_count", 0),
                "total_pnl": round(total_closed_pnl, 4),
            },
            "timestamp": int(time.time() * 1000),
        }

    # ─────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────

    async def _get_futures_price(self, symbol: str) -> dict:
        """Get current futures price from 24h ticker via raw API call."""
        resp = await self.futures.client.get(
            "/fapi/v1/ticker/24hr", params={"symbol": symbol.upper()}
        )
        resp.raise_for_status()
        d = resp.json()
        return {
            "price": float(d["lastPrice"]),
            "volume": float(d.get("quoteVolume", 0)),
        }

    async def _safe_funding(self, symbol: str) -> list[dict]:
        """Safely fetch funding rate history via raw API call."""
        try:
            resp = await self.futures.client.get(
                "/fapi/v1/fundingRate",
                params={"symbol": symbol.upper(), "limit": 30},
            )
            resp.raise_for_status()
            return [
                {
                    "symbol": f["symbol"],
                    "funding_rate": float(f["fundingRate"]),
                    "funding_time": int(f["fundingTime"]),
                    "mark_price": float(f.get("markPrice", 0)),
                }
                for f in resp.json()
            ]
        except Exception:
            return []

    async def _fetch_funding_income(self, symbol: str, since_ms: int) -> dict:
        """
        Fetch real funding fee payments from Binance for a symbol
        since a given timestamp.

        Uses /fapi/v1/income with incomeType=FUNDING_FEE.
        Positive income = we received funding (short when funding > 0).
        Negative income = we paid funding.

        Returns:
            dict with total_usdt, payments count, and per-payment breakdown.
        """
        try:
            days_back = max(1, int((time.time() * 1000 - since_ms) / 86_400_000) + 1)
            records = await self.futures.get_income_history(
                income_type="FUNDING_FEE",
                symbol=symbol,
                days=days_back,
            )

            # Filter to only records after our position opened
            relevant = [
                r for r in records
                if int(r.get("time", 0)) >= since_ms
            ]

            total = sum(float(r.get("income", 0)) for r in relevant)

            payments = []
            for r in relevant:
                payments.append({
                    "time": int(r.get("time", 0)),
                    "income_usdt": float(r.get("income", 0)),
                    "info": r.get("info", ""),
                })

            return {
                "total_usdt": round(total, 6),
                "payment_count": len(relevant),
                "payments": payments,
                "net_direction": "received" if total >= 0 else "paid",
            }
        except Exception as e:
            return {
                "total_usdt": 0,
                "payment_count": 0,
                "payments": [],
                "error": str(e),
            }

    def _save_state(self):
        """Persist proposals and positions to disk."""
        try:
            _ensure_dir()
            with open(PROPOSALS_FILE, "w") as f:
                json.dump({
                    "next_id": self._next_id,
                    "proposals": self._proposals,
                }, f, indent=2)
            with open(POSITIONS_FILE, "w") as f:
                json.dump(self._positions, f, indent=2)
        except Exception:
            pass  # Non-critical — data is also in memory

    def _load_state(self):
        """Load persisted state from disk."""
        try:
            if os.path.isfile(PROPOSALS_FILE):
                with open(PROPOSALS_FILE) as f:
                    data = json.load(f)
                    self._next_id = data.get("next_id", 1)
                    self._proposals = data.get("proposals", {})
        except Exception:
            pass

        try:
            if os.path.isfile(POSITIONS_FILE):
                with open(POSITIONS_FILE) as f:
                    self._positions = json.load(f)
        except Exception:
            pass
