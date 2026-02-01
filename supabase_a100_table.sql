-- ============================================================================
-- A100 GPU Index Price Table for Supabase
-- ============================================================================
-- Run this SQL in your Supabase SQL Editor to create the table

-- Create the a100_index_prices table
CREATE TABLE IF NOT EXISTS a100_index_prices (
    id BIGSERIAL PRIMARY KEY,
    
    -- Timestamp of when this index was calculated
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- The calculated weighted index price (USD per hour)
    index_price DECIMAL(10, 4) NOT NULL,
    
    -- Component breakdown
    hyperscaler_component DECIMAL(10, 4),
    neocloud_component DECIMAL(10, 4),
    
    -- Counts
    hyperscaler_count INTEGER,
    neocloud_count INTEGER,
    
    -- Validation metadata
    previous_price DECIMAL(10, 4),
    price_change_percent DECIMAL(6, 2),
    validation_passed BOOLEAN DEFAULT TRUE,
    
    -- Detailed JSON data (optional - for debugging)
    raw_data JSONB,
    
    -- Created timestamp
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create an index on recorded_at for faster queries
CREATE INDEX IF NOT EXISTS idx_a100_index_prices_recorded_at 
ON a100_index_prices(recorded_at DESC);

-- Create an index on created_at for ordering
CREATE INDEX IF NOT EXISTS idx_a100_index_prices_created_at 
ON a100_index_prices(created_at DESC);

-- Add a comment to document the table
COMMENT ON TABLE a100_index_prices IS 
'A100 GPU weighted index prices from multiple cloud providers';

COMMENT ON COLUMN a100_index_prices.index_price IS 
'Weighted average A100 GPU price in USD per hour';

COMMENT ON COLUMN a100_index_prices.price_change_percent IS 
'Percentage change from previous price entry';

COMMENT ON COLUMN a100_index_prices.validation_passed IS 
'Whether the price passed the ±20% validation check';

-- ============================================================================
-- Optional: Create a view for the latest price
-- ============================================================================
CREATE OR REPLACE VIEW latest_a100_index_price AS
SELECT 
    id,
    recorded_at,
    index_price,
    hyperscaler_component,
    neocloud_component,
    hyperscaler_count,
    neocloud_count,
    previous_price,
    price_change_percent,
    validation_passed,
    created_at
FROM a100_index_prices
ORDER BY created_at DESC
LIMIT 1;

-- ============================================================================
-- Optional: Function to get price history with change percentages
-- ============================================================================
CREATE OR REPLACE FUNCTION get_a100_price_history(limit_count INTEGER DEFAULT 30)
RETURNS TABLE (
    record_time TIMESTAMPTZ,
    price DECIMAL(10, 4),
    change_percent DECIMAL(6, 2),
    hs_component DECIMAL(10, 4),
    nc_component DECIMAL(10, 4)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.recorded_at,
        p.index_price,
        p.price_change_percent,
        p.hyperscaler_component,
        p.neocloud_component
    FROM a100_index_prices p
    WHERE p.validation_passed = TRUE
    ORDER BY p.created_at DESC
    LIMIT limit_count;
END;
$$ LANGUAGE plpgsql;
