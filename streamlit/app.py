# -*- coding: utf-8 -*-
# mypy: ignore-errors
import os

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import ApiClient
from databricks.sdk.service.iam import CurrentUserAPI

from posit.connect.external.databricks import (
    databricks_config,
    sql_credentials,
    ConnectStrategy,
)

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HOST_URL = f"https://{DATABRICKS_HOST}"
SQL_HTTP_PATH = os.getenv("DATABRICKS_PATH")

session_token = st.context.headers.get("Posit-Connect-User-Session-Token")
cfg = databricks_config(
    posit_connect_strategy=ConnectStrategy(user_session_token=session_token),
)

databricks_user = CurrentUserAPI(ApiClient(cfg)).me()
st.write(f"Hello, {databricks_user.display_name}!")

with sql.connect(
    server_hostname=DATABRICKS_HOST,
    http_path=SQL_HTTP_PATH,
    credentials_provider=sql_credentials(cfg),
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM samples.nyctaxi.trips LIMIT 10;")
        result = cursor.fetchall()
        st.table(pd.DataFrame(result))
