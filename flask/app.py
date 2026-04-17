# -*- coding: utf-8 -*-
# mypy: ignore-errors
import os

from databricks import sql
from flask import Flask, request

from posit.connect.external.databricks import (
    databricks_config,
    sql_credentials,
    ConnectStrategy,
)

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HOST_URL = f"https://{DATABRICKS_HOST}"
SQL_HTTP_PATH = os.getenv("DATABRICKS_PATH")

rows = None
app = Flask(__name__)


@app.route("/")
def usage():
    return "<p>Try: <pre>GET /fares<pre></p>"


@app.route("/fares")
def get_fares():
    global rows

    session_token = request.headers.get("Posit-Connect-User-Session-Token")
    cfg = databricks_config(
        posit_connect_strategy=ConnectStrategy(user_session_token=session_token),
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


if __name__ == "__main__":
    app.run(debug=True)
