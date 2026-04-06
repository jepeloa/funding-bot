-- 005: Add AEPS tracking fields to virtual_trades
ALTER TABLE virtual_trades ADD COLUMN IF NOT EXISTS partial_tp_triggered BOOLEAN DEFAULT FALSE;
ALTER TABLE virtual_trades ADD COLUMN IF NOT EXISTS etd_pct DOUBLE PRECISION;
ALTER TABLE virtual_trades ADD COLUMN IF NOT EXISTS atr_at_entry DOUBLE PRECISION;
ALTER TABLE virtual_trades ADD COLUMN IF NOT EXISTS time_to_mfe_secs DOUBLE PRECISION;
