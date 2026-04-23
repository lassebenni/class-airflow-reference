"""Final-state Week 11 taxi_pipeline (post-Chapter 5).

Reference snapshot students can diff their own ``dags/taxi_pipeline.py``
against if they get stuck. Ends up here after applying:

  - Chapter 4: three-task dependency chain (ingest -> dbt_run ->
    dbt_test), per-student schema isolation via ``AIRFLOW_STUDENT``,
    ``DBT_ENV`` injected through ``BashOperator.env``.
  - Chapter 5: monthly + catchup schedule, ``{{ ds }}``-templated
    download URL, delete-then-append idempotent load.
  - Chapter 7: ``response.raise_for_status()`` so a TLC 403/404 fails
    ``ingest_taxi_month`` cleanly instead of silently saving the HTML
    error body as a "parquet" and blowing up at dbt_run with a cryptic
    Parquet-magic-bytes error.
  - Chapter 8: runs on local Astro (Airflow 3 via Astro CLI) and on the
    shared class VM (same Airflow 3 via Docker Compose) without change.
    Picks up ``include/dbt_project/`` from either the Astro path or the
    Docker-Compose path via ``find_dbt_dir()`` below.

Design notes:

  - Download+load are one task (not two) because passing a filesystem
    path through XCom between tasks is fragile under Airflow 3's
    TaskSDK (the downstream task sees the path through the
    ExecutionAPI, and pandas/pyarrow can end up treating it as a
    Buffer instead of a file). Keeping both in one process sidesteps
    the whole class of issues: the parquet bytes live in local
    memory, get read by pandas directly, then land in Postgres.
  - dbt_run and dbt_test stay separate because they are genuinely
    distinct units of work — run materializes, test asserts. Seeing
    ``dbt_test`` red while ``dbt_run`` is green is a load-bearing
    signal for Ch7 debugging.

Requires:
  - Astro CLI 1.40+ (local) or the class shared VM (Airflow 3.2 on
    Docker Compose) with apache-airflow-providers-postgres,
    psycopg2-binary, pyarrow, pandas, requests available.
  - ``AIRFLOW_STUDENT`` env var set (Astro reads ``.env``; the VM
    takes it from the Docker Compose ``environment`` block) — picks
    the ``airflow_<name>`` schema this DAG writes into.
  - ``azure_pg`` Airflow Connection (seeded on the VM via bicep, or
    added manually in Astro's UI for local dev) pointing at
    hyf-data-pg.postgres.database.azure.com with ``sslmode=require``.
  - Week 10 dbt project mounted at ``include/dbt_project/``
    (Astro: ``/usr/local/airflow/include/dbt_project``;
    VM:    ``/opt/airflow/include/dbt_project``). The ``DBT_DIR``
    constant below auto-detects which one is present.
"""

import io
import os
from datetime import datetime

import pandas as pd
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, get_current_context, task

STUDENT = os.environ.get("AIRFLOW_STUDENT", "default")
SCHEMA = f"airflow_{STUDENT}"
TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def find_dbt_dir() -> str:
    """Return the dbt project path, picking whichever of the two known
    Airflow install roots actually has the project on disk.

    Astro CLI mounts ``include/`` at ``/usr/local/airflow/include``.
    The shared class VM's Docker Compose mounts it at
    ``/opt/airflow/include``. Students should not have to edit the DAG
    based on which environment they are running in, so we detect.
    """
    for base in ("/usr/local/airflow", "/opt/airflow"):
        candidate = f"{base}/include/dbt_project"
        if os.path.isdir(candidate):
            return candidate
    # Fall back to the Astro path so the DAG still parses if neither
    # exists yet (e.g. during first-push). The dbt tasks will fail
    # fast with a clear "project-dir not found" error, not a
    # silent-degrade, which is what we want.
    return "/usr/local/airflow/include/dbt_project"


DBT_DIR = find_dbt_dir()

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


def _ds_from_context() -> str:
    """Return the logical-date string for the current task run.

    TaskFlow's ``ds: str`` auto-injection works for *scheduled* runs
    (``logical_date`` is set to the interval boundary) but breaks for
    *manual* triggers in Airflow 3, where ``logical_date`` defaults to
    ``None``. Reading through ``get_current_context()`` with a
    ``run_after`` fallback is the form that works in both modes.
    """
    ctx = get_current_context()
    dr = ctx["dag_run"]
    dt = dr.logical_date or dr.run_after
    return dt.strftime("%Y-%m-%d")


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
    def ingest_taxi_month() -> int:
        """Download one month of TLC green-taxi data and load it into
        ``airflow_<student>.raw_trips`` in a single transaction.

        Kept as one task (rather than split into download + load)
        because passing a filesystem path between tasks through XCom
        is fragile under Airflow 3's TaskSDK. The parquet bytes stay
        in memory, get read by pandas directly, and are written to
        Postgres in the same process.
        """
        ds = _ds_from_context()
        year_month = ds[:7]

        # raise_for_status() converts a TLC 403/404 (future month,
        # typo'd path) into a visible Python exception. Without it,
        # the HTML error body would be saved and pandas would fail
        # later with a cryptic "Parquet magic bytes not found".
        resp = requests.get(parquet_url_for(ds), timeout=60)
        resp.raise_for_status()
        df = pd.read_parquet(io.BytesIO(resp.content))

        hook = PostgresHook(postgres_conn_id="azure_pg")
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

    ingest_taxi_month() >> dbt_run >> dbt_test


taxi_pipeline()
