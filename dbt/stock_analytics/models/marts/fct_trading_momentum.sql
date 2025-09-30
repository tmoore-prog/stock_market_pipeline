{{ config(
    materialized='incremental',
    unique_key=['ticker', 'trade_date'],
    partition_by={'field': 'trade_date', 'data_type': 'date'},
    cluster_by=['ticker'],
    on_schema_change='fail'
)}}

WITH base_metrics AS (
    SELECT 
        ticker,
        volume, 
        open,
        close,
        yesterday_close,
        high,
        low,
        trade_date,
        sector,
        company,
        index_weight,
        is_new_to_index,
        is_valid_record,
        {{ calculate_sma(20) }} as sma_20,
        {{ calculate_sma(50) }} as sma_50,
        {{ calculate_sma(200) }} as sma_200,
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
            THEN MIN(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END as low_52week,
        CASE 
            WHEN COUNT(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) >= 14
            THEN SUM(CASE WHEN close > yesterday_close THEN (close-yesterday_close) ELSE 0 END) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) / 14
            ELSE NULL
        END AS avg_gain_14,
        CASE 
            WHEN COUNT(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) >= 14
            THEN SUM(CASE WHEN close < yesterday_close THEN (yesterday_close-close) ELSE 0 END) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) / 14
            ELSE NULL
        END AS avg_loss_14
    FROM {{ ref('int_russell3000__daily') }}
),
signal_flags AS (
    SELECT
        *,
        CASE 
            WHEN close > sma_20 AND LAG(close) OVER (
                                        PARTITION BY ticker
                                        ORDER BY trade_date
                                    ) <= LAG(sma_20) OVER (
                                        PARTITION BY ticker
                                        ORDER BY trade_date
                                    )
            THEN 1
            ELSE 0
        END AS bullish_crossover,
        CASE 
            WHEN sma_50 > sma_200 AND LAG(sma_50) OVER (
                                        PARTITION BY ticker
                                        ORDER BY trade_date
                                    ) <= LAG(sma_200) OVER (
                                        PARTITION BY ticker
                                        ORDER BY trade_date
                                    )
            THEN 1
            ELSE 0
        END AS golden_cross,
        CASE
            WHEN sma_50 < sma_200 AND LAG(sma_50) OVER (
                                        PARTITION BY ticker
                                        ORDER BY trade_date
                                    ) >= LAG(sma_200) OVER (
                                        PARTITION BY ticker
                                        ORDER BY trade_date
                                    )
            THEN 1
            ELSE 0
        END AS death_cross,
        CASE 
            WHEN COUNT(volume) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW 
                ) >= 20
            THEN volume / (AVG(volume) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                )) 
            ELSE NULL
        END AS rel_vol,
        CASE 
            WHEN (avg_gain_14 IS NOT NULL AND avg_gain_14 != 0) AND (avg_loss_14 IS NOT NULL AND avg_loss_14 != 0)
            THEN 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
            ELSE NULL
        END AS rsi 
    FROM base_metrics
)

SELECT * 
FROM signal_flags
{% if is_incremental() %}
    WHERE trade_date >= (SELECT DATE_SUB(MAX(trade_date), INTERVAL 4 DAY) FROM {{ this }})
    AND is_valid_record = 1
{% endif %}

