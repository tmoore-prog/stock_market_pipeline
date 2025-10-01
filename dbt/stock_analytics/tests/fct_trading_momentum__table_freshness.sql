WITH dates as (
    SELECT 
        COUNT(trade_date) as recent_dates
        FROM {{ ref('fct_trading_momentum') }}
    WHERE trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)
)

SELECT 
    * 
FROM dates
WHERE recent_dates = 0
