WITH dates as (
    SELECT 
        COUNT(latest_trade_date) as recent_dates
        FROM {{ ref('dim_securities_current') }}
    WHERE latest_trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)
)

SELECT 
    * 
FROM dates
WHERE recent_dates = 0
