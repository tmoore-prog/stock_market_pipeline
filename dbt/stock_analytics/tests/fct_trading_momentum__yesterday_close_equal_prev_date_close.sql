
WITH agg AS (
    SELECT 
        *,
        LAG(close, 1) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
        ) as lag_close
    FROM {{ ref('fct_trading_momentum') }}
)

SELECT 
    *
FROM agg
WHERE 
    yesterday_close IS NOT NULL AND
    yesterday_close != lag_close AND
    trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)