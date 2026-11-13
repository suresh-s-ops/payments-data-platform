"""Database cursors for reconciliation. Credentials come from the environment
(injected by the scheduler's secrets backend), never from code."""
import os

import oracledb
import snowflake.connector


def legacy_cursor():
    conn = oracledb.connect(user=os.environ["LEGACY_USER"],
                            password=os.environ["LEGACY_PASSWORD"],
                            dsn=os.environ["LEGACY_DSN"])
    return conn.cursor()


def snowflake_cursor():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_KEY_PATH"],
        warehouse="TRANSFORM_WH",
    )
    return conn.cursor()
