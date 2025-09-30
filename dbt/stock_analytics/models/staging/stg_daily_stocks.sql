SELECT 
    T as ticker,
    CAST(v AS INTEGER) as volume,
    vw as volume_weighted_avg,
    o as open,
    c as close,
    h as high,
    l as low,
    n as num_transactions,
    date as trade_date,
    ingested_at,
    CASE WHEN v > 0.0 
          THEN 1 
          ELSE 0
          END AS has_volume,
    CASE WHEN o > 0.0
            AND c > 0.0
            AND h > 0.0
            AND l > 0.0
            AND c <= h
            AND c >= l
            AND l <= h
          THEN 1 
          ELSE 0
          END AS is_valid_record
FROM {{ source('raw_market', 'daily_stocks') }}

