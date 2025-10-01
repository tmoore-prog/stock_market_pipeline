SELECT 
    *
FROM {{ ref('agg_daily_market_breadth') }}
WHERE record_high_pct > 0.3    -- Over 30% of market hitting record highs is extremely unlikely
    AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
