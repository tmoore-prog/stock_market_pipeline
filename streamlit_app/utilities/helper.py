from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st


credentials = service_account.Credentials.from_service_account_info(
    st.secrets['gcp_service_account']
)
client = bigquery.Client(credentials=credentials)


def query_bigquery(sql):
    return client.query(sql).to_dataframe()
