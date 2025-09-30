{{ config(
    materialized='table'
)}}


WITH latest_snapshot AS (
    SELECT
        ticker,
        company,
        sector, 
        trade_date as latest_trade_date,
        volume as latest_volume,
        open as latest_open,
        close as latest_close,
        yesterday_close as latest_prev_close,
        high as latest_high,
        low as latest_low,
        sma_20 as latest_sma20,
        sma_50 as latest_sma50,
        sma_200 as latest_sma200,
        rsi as latest_rsi,
        rel_vol as latest_rel_vol,
        high_52week as latest_52week_high,
        low_52week as latest_52week_low,
        (close - yesterday_close) as price_change_1d,
        (close - yesterday_close) / yesterday_close as return_1d
    FROM {{ ref('fct_trading_momentum') }}
    WHERE trade_date = (SELECT MAX(trade_date) FROM {{ ref('fct_trading_momentum') }})
),

returns_lookback AS (
    SELECT 
        ticker,
        {{ calculate_return(5) }} as return_1w,
        {{ calculate_return(21) }} as return_1m,
        {{ calculate_return(63) }} as return_3m,
        {{ calculate_return(252) }} as return_ytd,
    FROM {{ ref('fct_trading_momentum') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) = 1
),

numbered_dates AS (
    SELECT 
        ticker,
        sector,
        trade_date,
        close, 
        yesterday_close,
        volume,
        ROW_NUMBER() OVER (
            PARTITION BY ticker
            ORDER BY trade_date DESC
        ) as days_back
    FROM {{ ref('fct_trading_momentum') }}
    WHERE trade_date >= DATE_SUB((SELECT MAX(trade_date) FROM {{ ref('fct_trading_momentum') }}), INTERVAL 33 DAY)
), 

sector_lookback AS (
    SELECT 
        ticker,
        sector,
        trade_date,
        {{ calculate_return(21) }} as return_1m
    FROM numbered_dates
),

sector_metrics AS (
    SELECT
        ticker,
        AVG(return_1m) OVER (
            PARTITION BY sector
        ) as sector_return_1m,
        CASE
            WHEN return_1m IS NOT NULL
            THEN PERCENT_RANK() OVER (
                    PARTITION BY (CASE WHEN return_1m IS NOT NULL THEN 1 ELSE 0 END)
                    ORDER BY return_1m)
            ELSE NULL
        END as performance_percentile
    FROM sector_lookback
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) = 1
),

volatility_metrics AS (
    SELECT
        ticker,
        STDDEV(LN(close / yesterday_close)) * SQRT(252) as volatility_20d,
        AVG(volume) as avg_volume_20d,
        COUNT(*) as trading_days
    FROM numbered_dates
    WHERE days_back <= 20
    GROUP BY ticker
),

trading_days_count AS (
    SELECT
        ticker, 
        COUNT(DISTINCT trade_date) as total_trading_days
    FROM {{ ref('fct_trading_momentum') }}
    GROUP BY ticker 
),

signal_flags AS (
    SELECT
        ticker,
        CASE
            WHEN l.latest_sma50 > l.latest_sma200
            THEN 1
            ELSE 0
        END as has_golden_cross_active,
        CASE
            WHEN l.latest_close > l.latest_sma20
            THEN 1
            ELSE 0
        END as over_sma20,
        CASE
            WHEN l.latest_close > l.latest_sma50
            THEN 1
            ELSE 0
        END as over_sma50,
        CASE
            WHEN l.latest_close > l.latest_sma200
            THEN 1
            ELSE 0
        END as over_sma200
    FROM latest_snapshot as l
),

last_signals AS (
    SELECT 
        ticker,
        COALESCE(
            MAX(CASE
                    WHEN golden_cross = 1
                    THEN trade_date
                END),
            MIN(CASE 
                    WHEN sma_200 IS NOT NULL
                    THEN trade_date
                END)) as last_golden_cross,
        COALESCE(
            MAX(CASE
                    WHEN close > sma_50 AND (yesterday_close < sma_50 OR yesterday_close IS NULL)
                    THEN trade_date
                END),
            MIN(CASE 
                    WHEN sma_50 IS NOT NULL AND close > sma_50 
                    THEN trade_date 
                END)) as day_cross_over_sma50,
        COALESCE(
            MAX(CASE
                    WHEN close < sma_50 AND (yesterday_close > sma_50 OR yesterday_close IS NULL)
                    THEN trade_date
                END),
            MIN(CASE 
                    WHEN sma_50 IS NOT NULL AND close < sma_50 
                    THEN trade_date 
                END)) as day_cross_below_sma50,
    FROM {{ ref('fct_trading_momentum') }} 
    WHERE trade_date >= DATE_SUB((SELECT MAX(trade_date) FROM {{ ref('fct_trading_momentum') }}), INTERVAL 365 DAY)
    GROUP BY ticker     
),

final AS (
    SELECT 
        l.*,
        CASE
            WHEN latest_52week_high IS NOT NULL
            THEN (latest_52week_high - latest_close) / latest_52week_high
            ELSE NULL
        END as pct_distance_from_52week_high,
        CASE 
            WHEN latest_52week_low IS NOT NULL
            THEN (latest_close - latest_52week_low) / latest_52week_low
            ELSE NULL
        END as pct_distance_from_52week_low,
        t_days.total_trading_days,
        r.return_1w,
        r.return_1m,
        r.return_3m,
        r.return_ytd,
        sm.sector_return_1m,
        sm.performance_percentile,
        CASE    
            WHEN r.return_1m IS NOT NULL
            THEN (r.return_1m - sm.sector_return_1m)
        END as outperformance_vs_sector,
        CASE
            WHEN v.trading_days >= 20 
            THEN v.volatility_20d
            ELSE NULL
        END as volatility_20d,
        CASE 
            WHEN v.trading_days >= 20
            THEN v.avg_volume_20d
            ELSE NULL
        END AS avg_volume_20d,
        s.has_golden_cross_active,
        s.over_sma20,
        s.over_sma50,
        s.over_sma200,
        DATE_DIFF(l.latest_trade_date, ls.last_golden_cross, DAY) as days_since_last_golden_cross,
        CASE
            WHEN s.over_sma50 = 1
            THEN DATE_DIFF(l.latest_trade_date, ls.day_cross_over_sma50, DAY) 
            ELSE NULL
        END as days_over_sma50,
        CASE
            WHEN s.over_sma50 = 0
            THEN DATE_DIFF(l.latest_trade_date, ls.day_cross_below_sma50, DAY) 
            ELSE NULL
        END as days_under_sma50
    FROM latest_snapshot as l
    LEFT JOIN returns_lookback as r
    ON l.ticker = r.ticker
    LEFT JOIN trading_days_count as t_days
    ON l.ticker = t_days.ticker
    LEFT JOIN volatility_metrics as v
    ON l.ticker = v.ticker
    LEFT JOIN signal_flags as s
    ON l.ticker = s.ticker
    LEFT JOIN last_signals as ls 
    ON l.ticker = ls.ticker
    LEFT JOIN sector_metrics as sm
    ON l.ticker = sm.ticker
)

SELECT * FROM final
