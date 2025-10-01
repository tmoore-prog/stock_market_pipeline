SELECT 
    *
FROM {{ ref('fct_trading_momentum') }}
WHERE close > high_52week OR close < low_52week
    AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)