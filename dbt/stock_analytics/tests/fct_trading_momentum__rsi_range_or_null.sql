SELECT *
FROM {{ ref('fct_trading_momentum') }}
WHERE 
    rsi IS NOT NULL 
    AND (rsi < 0 OR rsi > 100)
