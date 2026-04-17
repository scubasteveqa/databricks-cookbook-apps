# -*- coding: utf-8 -*-
# mypy: ignore-errors
import os
from typing import Annotated

from databricks import sql
from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse

from posit.connect.external.databricks import (
    databricks_config,
    sql_credentials,
    ConnectStrategy,
)

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HOST_URL = f"https://{DATABRICKS_HOST}"
SQL_HTTP_PATH = os.getenv("DATABRICKS_PATH")

rows = None
app = FastAPI()


@app.get("/fares")
async def get_fares(
    posit_connect_user_session_token: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    global rows

    cfg = databricks_config(
        posit_connect_strategy=ConnectStrategy(
            user_session_token=posit_connect_user_session_token
        ),
    )

    if rows is None:
        query = "SELECT * FROM samples.nyctaxi.trips LIMIT 10;"

        with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=SQL_HTTP_PATH,
            credentials_provider=sql_credentials(cfg),
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

    return [row.asDict() for row in rows]
