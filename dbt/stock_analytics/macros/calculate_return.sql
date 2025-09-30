{% macro calculate_return(periods) %}

    CASE 
        WHEN COUNT(close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
        ) >= {{ periods }}
        THEN (close - LAG(close, {{ periods }}) OVER (
            PARTITION BY ticker 
            ORDER BY trade_date
            )) / LAG(close, {{ periods }}) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
        )
        ELSE NULL
    END

{% endmacro %}
