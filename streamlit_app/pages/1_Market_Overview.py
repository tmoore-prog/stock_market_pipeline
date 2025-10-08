from utilities.helper import query_bigquery
import streamlit as st


st.title('Market Overview')


query = '''
        SELECT *
        FROM dbt-learning-project-471822.analytics.agg_daily_market_breadth
        ORDER BY trade_date DESC
        LIMIT 30
    '''

df = query_bigquery(query)
chart_df = df.sort_values('trade_date', ascending=True)


def make_column_metric(metric, col, percent=False):
    title = metric.replace('_', ' ').title()
    latest_metric = df[metric].iloc[0]
    prev_metric = df[metric].iloc[1]
    if percent:
        latest_metric = latest_metric * 100
        prev_metric = prev_metric * 100
    metric_change = latest_metric - prev_metric
    metric_series = chart_df[metric].tolist()
    if percent:
        return col.metric(title, f'{latest_metric:.2f}%',
                          f'{metric_change:.2f}%', chart_data=metric_series)
    else:
        return col.metric(title, f'{latest_metric:.2f}',
                          f'{metric_change:.2f}', chart_data=metric_series)


col1, col2, col3 = st.columns(3)

make_column_metric('pct_market_over_sma50', col1, percent=True)
make_column_metric('high_low_index', col2, percent=True)
make_column_metric('market_rsi', col3)

col4, col5, col6 = st.columns(3)

make_column_metric('ad_ratio', col4)
make_column_metric('ad_line', col5)
make_column_metric('up_down_volume_ratio', col6)

st.caption('Up/Down Volume Ratio is a single-day metric rather than '
           'the common 50-day rolling ratio')
