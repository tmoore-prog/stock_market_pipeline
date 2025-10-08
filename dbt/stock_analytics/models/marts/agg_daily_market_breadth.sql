{{ config(
    materialized='table'
)}}

WITH base_aggregates AS (
    SELECT 
        trade_date,
        COUNT(DISTINCT ticker) as stocks_traded,
        SUM(CASE
                WHEN close = yesterday_close OR yesterday_close IS NULL
                THEN 1 
                ELSE 0
                END
        ) as unchanged_stocks,
        SUM(CASE
                WHEN close > yesterday_close AND yesterday_close IS NOT NULL
                THEN 1
                ELSE 0
                END
        ) as advances,
        SUM(CASE
                WHEN close < yesterday_close AND yesterday_close IS NOT NULL
                THEN 1
                ELSE 0
                END 
        ) as declines,
        SUM(CASE
                WHEN close > yesterday_close AND yesterday_close IS NOT NULL
                THEN volume
                ELSE 0
                END
        ) as up_volume,
        SUM(CASE
                WHEN close < yesterday_close AND yesterday_close IS NOT NULL
                THEN volume
                ELSE 0
                END
        ) as down_volume
    FROM {{ ref('int_russell3000__daily') }}
    GROUP BY trade_date
),

rolling_high_low AS (
    SELECT
        *,
        CASE
            WHEN COUNT(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) >= 252
            THEN MAX(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) 
            ELSE NULL
            END as high_52week,
        CASE
            WHEN COUNT(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) >= 252
            THEN
            MIN(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) 
            ELSE NULL
            END as low_52week
    FROM {{ ref('int_russell3000__daily') }}
),

high_low_aggs AS (
    SELECT 
        trade_date,
        SUM(CASE WHEN close = high_52week THEN 1 ELSE 0 END) as new_highs,
        SUM(CASE WHEN close = low_52week THEN 1 ELSE 0 END) as new_lows
    FROM rolling_high_low
    GROUP BY trade_date
),

sma_aggs AS (
    SELECT
        trade_date, 
        SUM(CASE WHEN close > sma_20 THEN 1 ELSE 0 END) / COUNT(close) as pct_market_over_sma20,
        SUM(CASE WHEN close > sma_50 THEN 1 ELSE 0 END) / COUNT(close) as pct_market_over_sma50,
        SUM(CASE WHEN close > sma_200 THEN 1 ELSE 0 END) / COUNT(close) as pct_market_over_sma200,
        AVG(rsi) as market_rsi
    FROM {{ ref('fct_trading_momentum') }}
    GROUP BY trade_date
),

all_aggs AS (
    SELECT 
        b.*,
        s.pct_market_over_sma20,
        s.pct_market_over_sma50,
        s.pct_market_over_sma200,
        s.market_rsi,
        SUM(b.advances - b.declines) OVER (
            ORDER BY b.trade_date
        ) as ad_line,
        CASE 
            WHEN (b.advances + b.declines + b.unchanged_stocks) > 0
            THEN ((b.advances - b.declines) / (b.advances + b.declines + b.unchanged_stocks)) 
            ELSE NULL
        END as ad_percentage,
        SAFE_DIVIDE(b.advances, b.declines) as ad_ratio,
        CASE
            WHEN (b.up_volume IS NOT NULL AND b.up_volume != 0) AND (b.down_volume IS NOT NULL AND b.down_volume != 0)
            THEN b.up_volume / b.down_volume
            ELSE NULL
        END as up_down_volume_ratio,
        CASE
            WHEN s.market_rsi > 70 
            THEN 'overbought'
            WHEN s.market_rsi < 30
            THEN 'oversold'
            ELSE 'normal'
        END as market_momentum,
        CASE 
            WHEN b.stocks_traded > 0
            THEN h.new_highs / b.stocks_traded
            ELSE NULL
        END as record_high_pct,
        AVG(CASE WHEN (h.new_highs + h.new_lows) > 0
                    THEN h.new_highs / (h.new_highs + h.new_lows)
                    ELSE NULL END) OVER (
                        ORDER BY h.trade_date
                        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) as high_low_index

    FROM base_aggregates as b
    LEFT JOIN sma_aggs as s
    ON s.trade_date = b.trade_date
    LEFT JOIN high_low_aggs as h
    ON h.trade_date = b.trade_date
)

SELECT * FROM all_aggs
ORDER BY trade_date
