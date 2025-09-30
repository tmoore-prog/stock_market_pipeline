WITH russell_snapshots AS (
    SELECT 
        Ticker as ticker,
        Name as company, 
        Sector as sector,
        `Market Value` as market_value,
        Weight as market_weight,
        DATE('2023-01-01') as valid_from,
        DATE('2025-06-29') as valid_to
    FROM {{ source('staging_russell_3000', 'russell3000_2024_1231') }}

    UNION ALL

    SELECT 
        Ticker as ticker,
        Name as company, 
        Sector as sector,
        `Market Value` as market_value,
        Weight as market_weight,
        DATE('2025-06-30') as valid_from,
        DATE('2025-08-28') as valid_to
    FROM {{ source('staging_russell_3000', 'russell3000_2025_0630') }}

    UNION ALL

    SELECT 
        Ticker as ticker,
        Name as company, 
        Sector as sector,
        `Market Value` as market_value,
        Weight as market_weight,
        DATE('2025-08-29') as valid_from,
        DATE('2025-09-15') as valid_to
    FROM {{ source('staging_russell_3000', 'russell3000_2025_0829') }}

    UNION ALL

    SELECT 
        Ticker as ticker,
        Name as company, 
        Sector as sector,
        `Market Value` as market_value,
        Weight as market_weight,
        DATE('2025-09-16') as valid_from,
        DATE('3000-01-01') as valid_to
    FROM {{ source('staging_russell_3000', 'russell3000_2025_0916') }}
)

SELECT DISTINCT * FROM russell_snapshots