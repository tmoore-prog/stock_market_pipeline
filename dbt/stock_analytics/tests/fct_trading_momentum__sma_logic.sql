SELECT *
FROM {{ ref('fct_trading_momentum') }}
WHERE ((sma_200 IS NOT NULL AND sma_50 IS NULL)
    OR (sma_200 IS NOT NULL AND sma_20 IS NULL)
    OR (sma_50 IS NOT NULL AND sma_20 IS NULL))
    AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
