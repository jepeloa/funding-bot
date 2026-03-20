-- Add trading_mode column to distinguish live vs paper trades
ALTER TABLE virtual_trades ADD COLUMN IF NOT EXISTS trading_mode TEXT NOT NULL DEFAULT 'paper';
