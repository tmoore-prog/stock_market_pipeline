import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account


credentials = service_account.Credentials.from_service_account_info(
    st.secrets['gcp_service_account']
)
client = bigquery.Client(credentials=credentials)


def query_bigquery(sql):
    return client.query(sql).to_dataframe()


sector_query = '''SELECT DISTINCT sector \
        FROM dbt-learning-project-471822.analytics.dim_securities_current \
        ORDER BY sector'''
all_sectors = query_bigquery(sector_query)['sector'].tolist()

st.sidebar.header("Filters")
rsi_min, rsi_max = st.sidebar.slider("RSI Range", 0, 100, (20, 80))
sector = st.sidebar.multiselect("Sectors", options=all_sectors)
signals = st.sidebar.multiselect("Signals", ["Golden Cross", "Death Cross"])

query = f"""
    SELECT ticker, company, sector, latest_rsi, latest_close, return_1m
    FROM dbt-learning-project-471822.analytics.dim_securities_current
    WHERE latest_rsi BETWEEN {rsi_min} AND {rsi_max}
    {'AND sector IN ("' + '","'.join(sector) + '")' if sector else ''}
    ORDER BY return_1m DESC
"""

results = query_bigquery(query)

st.dataframe(results, width='stretch')
