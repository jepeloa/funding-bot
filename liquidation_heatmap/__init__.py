"""
Liquidation Heatmap Engine — GPU-accelerated estimation of liquidation levels.

Estimates where open positions would be liquidated based on:
  - Volume profile from ohlcv_1m continuous aggregate
  - Open interest magnitude
  - Assumed leverage distribution (5x–100x)
  - Realized liquidations subtracted

Architecture:
  Standalone FastAPI micro-service (port 8009), proxied by main API.
  Uses CuPy (CUDA) on 2x RTX 3060 Ti for parallel computation.
"""
