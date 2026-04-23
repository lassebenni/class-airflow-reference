"""Final-state Week 11 taxi_pipeline (post-Chapter 5).

Reference snapshot students can diff their own ``dags/taxi_pipeline.py``
against if they get stuck. Ends up here after applying:

  - Chapter 4: four-task dependency chain (download -> load -> dbt_run
    -> dbt_test), per-student schema isolation via ``AIRFLOW_STUDENT``,
    ``DBT_ENV`` injected through ``BashOperator.env``.
  - Chapter 5: monthly + catchup schedule, ``{{ ds }}``-templated
    download URL, delete-then-append idempotent load.

Requires:
  - Astro CLI 1.40+ with apache-airflow-providers-postgres,
    psycopg2-binary, pyarrow, dbt-core==1.10.*, dbt-postgres==1.10.*
    in requirements.txt.
  - AIRFLOW_STUDENT env var set in .env (picks the airflow_<name>
    schema this DAG writes into).
  - ``azure_pg`` Airflow Connection pointing at
    hyf-data-pg.postgres.database.azure.com with sslmode=require.
  - Week 10 dbt project at ``include/dbt_project/`` (or the
    ``week-11-airflow`` branch of ``nyc-taxi-dbt-reference``).
"""

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

STUDENT = os.environ.get("AIRFLOW_STUDENT", "default")
SCHEMA = f"airflow_{STUDENT}"
DBT_DIR = "/usr/local/airflow/include/dbt_project"
TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DBT_ENV = {
    "PG_HOST": "{{ conn.azure_pg.host }}",
    "PG_USER": "{{ conn.azure_pg.login }}",
    "PG_PASSWORD": "{{ conn.azure_pg.password }}",
    "PG_DBNAME": "{{ conn.azure_pg.schema }}",
    "PG_SCHEMA": SCHEMA,
}


def parquet_url_for(ds: str) -> str:
    """Return the TLC green-taxi parquet URL for a logical date.

    Pure function, extracted from ``download_taxi_month`` so it can be
    unit-tested without an Airflow runtime (see Chapter 6).

    >>> parquet_url_for("2024-01-01")
    'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet'
    """
    year_month = ds[:7]  # "2024-01-01" -> "2024-01"
    return f"{TLC_BASE}/green_tripdata_{year_month}.parquet"


@dag(
    dag_id="lasse_taxi_pipeline",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,   # serialize: concurrent dbt runs collide on __dbt_backup relations
    default_args={"retries": 2},   # retry transient failures twice before marking the task failed
    tags=["week11", "taxi", "student:lasse"],
)
def taxi_pipeline():
    @task()
    def download_taxi_month(ds: str) -> str:
        year_month = ds[:7]
        path = f"/tmp/green_tripdata_{year_month}.parquet"
        Path(path).write_bytes(requests.get(parquet_url_for(ds), timeout=60).content)
        return path

    @task()
    def load_raw_trips(parquet_path: str, ds: str) -> int:
        df = pd.read_parquet(parquet_path)
        hook = PostgresHook(postgres_conn_id="azure_pg")
        year_month = ds[:7]
        # DELETE runs through psycopg's raw cursor; to_sql below uses
        # SQLAlchemy's engine, because pandas.to_sql wants an engine.
        # Two APIs, same connection pool under the hood.
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
            cur.execute(
                f'DELETE FROM "{SCHEMA}".raw_trips '
                "WHERE to_char(lpep_pickup_datetime, 'YYYY-MM') = %s",
                (year_month,),
            )
        df.to_sql(
            "raw_trips",
            hook.get_sqlalchemy_engine(),
            schema=SCHEMA,
            if_exists="append",
            index=False,
        )
        return len(df)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env=DBT_ENV,
        append_env=True,
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env=DBT_ENV,
        append_env=True,
    )

    load_raw_trips(download_taxi_month()) >> dbt_run >> dbt_test


taxi_pipeline()
