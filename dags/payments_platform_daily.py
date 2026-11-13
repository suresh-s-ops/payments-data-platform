from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from quality.reconcile import reconcile
from quality.connections import legacy_cursor, snowflake_cursor

VOLUME_TOLERANCE = 0.30   # alert if daily volume moves >30% vs. 14-day average


def volume_gate(ds, **_):
    """A sudden drop in payment volume usually means a broken processor feed,
    not a real business change, so stop before reports publish."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_platform")
    today, avg14 = hook.get_first(f"""
        WITH d AS (
          SELECT DATE(event_ts) AS dt, COUNT(*) AS n
          FROM silver.payments.payment_attempts
          WHERE DATE(event_ts) BETWEEN DATEADD(day, -14, '{ds}') AND '{ds}'
          GROUP BY 1)
        SELECT MAX(IFF(dt = '{ds}', n, NULL)), AVG(IFF(dt < '{ds}', n, NULL)) FROM d
    """)
    if not today or abs(today - avg14) / avg14 > VOLUME_TOLERANCE:
        raise AirflowFailException(f"Volume anomaly {ds}: {today} vs 14d avg {avg14:.0f}")


def recon_gate(ds, **_):
    result = reconcile(legacy_cursor(), snowflake_cursor(), ds)
    if not result.passed:
        raise AirflowFailException(f"Reconciliation failed: {result}")


with DAG(
    dag_id="payments_platform_daily",
    start_date=datetime(2026, 1, 1),
    schedule="30 5 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "payments-data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "sla": timedelta(hours=2),
        "email_on_failure": True,
        "email": ["payments-data-oncall@example.com"],
    },
    tags=["payments", "platform", "migration"],
) as dag:

    extract_legacy = SparkSubmitOperator(
        task_id="extract_legacy_settlements",
        application="spark/extract_legacy_settlements.py",
        conn_id="hadoop_spark",
        application_args=["--watermark-from", "{{ ds }} 00:00:00",
                          "--watermark-to", "{{ next_ds }} 00:00:00"],
    )

    stage_to_snowflake = BashOperator(
        task_id="stage_settlements",
        bash_command=(
            "hdfs dfs -get -f /data/raw/payments/settlements/dt={{ ds }} /tmp/stl_{{ ds }} && "
            "snowsql -c platform -q \"PUT file:///tmp/stl_{{ ds }}/*.parquet "
            "@raw.payments.settlements_stage/dt={{ ds }}/ AUTO_COMPRESS=FALSE\""
        ),
    )

    load_raw = SnowflakeOperator(
        task_id="copy_settlements",
        snowflake_conn_id="snowflake_platform",
        sql="snowflake/load_settlements.sql",
    )

    check_volume = PythonOperator(task_id="volume_gate", python_callable=volume_gate)

    dbt_build = BashOperator(
        task_id="dbt_build_gold_payments",
        bash_command="cd /opt/dbt && dbt build --select tag:payments tag:collections --target prod",
    )

    check_recon = PythonOperator(task_id="reconciliation_gate",
                                 python_callable=recon_gate)

    (extract_legacy >> stage_to_snowflake >> load_raw
        >> check_volume >> dbt_build >> check_recon)
