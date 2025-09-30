SELECT *
FROM {{ ref('fct_trading_momentum') }}
WHERE golden_cross = 1 AND death_cross = 1