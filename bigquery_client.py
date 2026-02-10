from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st


def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )

    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id
    )

    return client
