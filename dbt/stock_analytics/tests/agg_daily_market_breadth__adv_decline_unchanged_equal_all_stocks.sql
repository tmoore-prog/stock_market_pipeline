SELECT 
    *
FROM {{ ref('agg_daily_market_breadth') }}
WHERE (advances + declines + unchanged_stocks) != stocks_traded