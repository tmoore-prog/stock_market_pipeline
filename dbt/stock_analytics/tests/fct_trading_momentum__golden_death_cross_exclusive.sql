SELECT *
FROM {{ ref('fct_trading_momentum') }}
WHERE golden_cross = 1 AND death_cross = 1
    AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)