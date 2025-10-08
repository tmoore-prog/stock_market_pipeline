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


st.dataframe(df, width='stretch')
