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

pct = df['pct_market_over_sma50'].iloc[0] * 100
pct_change = pct - (df['pct_market_over_sma50'].iloc[1] * 100)
pct_chart = chart_df['pct_market_over_sma50'].tolist()
hl_index = df['high_low_index'].iloc[0] * 100
hl_index_change = hl_index - (df['high_low_index'].iloc[1] * 100)
rsi = df['market_rsi'].iloc[0]
rsi_change = rsi - (df['market_rsi'].iloc[1])
hl_chart = chart_df['high_low_index'].tolist()
rsi_chart = chart_df['market_rsi'].tolist()
ad_ratio = df['ad_ratio'].iloc[0]
ad_ratio_change = ad_ratio - (df['ad_ratio'].iloc[1])
ad_ratio_chart = chart_df['ad_ratio'].tolist()
ad_line = df['ad_line'].iloc[0]
ad_line_change = ad_ratio - (df['ad_line'].iloc[1])
ad_line_chart = chart_df['ad_line'].tolist()
udv_ratio = df['up_down_volume_ratio'].iloc[0]
udv_ratio_change = udv_ratio - df['up_down_volume_ratio'].iloc[1]
udv_chart = chart_df['up_down_volume_ratio'].tolist()

col1, col2, col3 = st.columns(3)

col1.metric("Percent Market Over SMA50", f'{pct:.2f}%', f'{pct_change:.2f}%', chart_data=pct_chart)
col2.metric("High/Low Index", f'{hl_index:.2f}%', f'{hl_index_change:.2f}%', chart_data=hl_chart)
col3.metric("Market RSI", f'{rsi:.2f}%', f'{rsi_change:.2f}%', chart_data=rsi_chart)


col4, col5, col6 = st.columns(3)

col4.metric("A/D Ratio", f'{ad_ratio:.2f}', f'{ad_ratio_change:.2f}', chart_data=ad_ratio_chart)
col5.metric("A/D Line", f'{ad_line:.2f}', f'{ad_line_change:.2f}', chart_data=ad_line_chart)
col6.metric("Up/Down Volume Ratio", f'{udv_ratio:.2f}', f'{udv_ratio_change:.2f}', chart_data=udv_chart)

st.caption('Up/Down Volume Ratio is a single-day metric rather than the common 50-day rolling ratio')
