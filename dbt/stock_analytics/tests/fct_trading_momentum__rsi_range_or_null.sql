SELECT *
FROM {{ ref('fct_trading_momentum') }}
WHERE 
    rsi IS NOT NULL 
    AND (rsi < 0 OR rsi > 100)
    AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
