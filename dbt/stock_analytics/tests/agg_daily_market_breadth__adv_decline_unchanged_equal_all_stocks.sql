SELECT 
    *
FROM {{ ref('agg_daily_market_breadth') }}
WHERE (advances + declines + unchanged_stocks) != stocks_traded
    AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)