{{ config(
    materialized='incremental',
    unique_key=['ticker', 'trade_date'],
    partition_by={'field': 'trade_date', 'data_type': 'date'},
    cluster_by=['ticker'],
    on_schema_change='fail'
) }}

WITH russell_3000 AS (
    SELECT * FROM {{ ref('stg_russell_3000__constituents') }}
),

full_market AS (
    SELECT * FROM {{ ref('stg_daily_stocks') }}
     {% if is_incremental() %}

    WHERE trade_date >= (SELECT DATE_SUB(MAX(trade_date), INTERVAL 4 DAY) FROM {{ this }})

    {% endif %}
),

russell3000_daily AS (
    SELECT 
        full_market.*,
        russell_3000.sector,
        russell_3000.company,
        russell_3000.market_weight as index_weight
    FROM full_market
    INNER JOIN russell_3000
        ON full_market.ticker = russell_3000.ticker 
        AND full_market.trade_date BETWEEN russell_3000.valid_from AND russell_3000.valid_to
),

prev_closes AS (
    SELECT 
        ticker, 
        trade_date,
        LAG(close, 1) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
        ) as prev_close
    FROM {{ this }}
    WHERE trade_date >= (SELECT DATE_SUB(MIN(trade_date), INTERVAL 10 DAY) FROM full_market)
)

SELECT 
    r.*,
    ROW_NUMBER() OVER (
        PARTITION BY r.ticker
        ORDER BY r.trade_date
    ) as consecutive_trading_days,
    COALESCE(
        LAG(r.close, 1) OVER (
            PARTITION BY r.ticker
            ORDER BY r.trade_date
        ), p.prev_close
    ) as yesterday_close,
    CASE 
        WHEN LAG(r.ticker) OVER (PARTITION BY r.ticker ORDER BY r.trade_date) IS NULL 
        THEN 1 
        ELSE 0 
    END as is_new_to_index
FROM russell3000_daily as r
LEFT JOIN prev_closes as p
    ON r.ticker = p.ticker   
    AND r.trade_date = p.trade_date
