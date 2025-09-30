{% macro calculate_sma(periods) %}

    CASE 
        WHEN COUNT(close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
        ) >= {{ periods }}
        THEN AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN {{ periods - 1 }}  PRECEDING AND CURRENT ROW
        )
        ELSE NULL
    END

{% endmacro %}

