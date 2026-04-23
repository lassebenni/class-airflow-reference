# nyc-taxi-dbt-reference

Reference dbt project for **HYF Data Track Week 10**. Mirrors the `nyc_taxi` project that the
chapters in `Data Track/Week 10/` walk you through building.

This repo is a **safety net**, not a starter. The chapters expect you to type each file
yourself. If you fall behind or something breaks, check out the tag for the chapter you are
on and diff your local copy against this one.

## Tags (one per chapter checkpoint)

| Tag                     | Chapter                                  | What you should have at this point                                  |
| ----------------------- | ---------------------------------------- | ------------------------------------------------------------------- |
| `ch2-dbt-setup-azure`   | Ch 2 — dbt Setup for Azure PostgreSQL    | `dbt_project.yml`, `profiles.yml.example`, `_sources.yml`, plain `stg_trips.sql` |
| `ch3-sql-jinja`         | Ch 3 — SQL and Jinja Templating          | `stg_trips.sql` extended with `payment_type_label`, `tip_pct`, `fare_per_mile` |
| `ch4-materializations`  | Ch 4 — Materializations & Layers         | `stg_zones.sql`, `fct_trips.sql`, materialization config in `dbt_project.yml` |
| `ch5-dbt-tests`         | Ch 5 — dbt Tests                         | All schema YAML, singular test, unit test, `packages.yml`           |
| `ch6-docs-extras`       | Ch 6 — Docs & Extras                     | Fleshed-out `_fct_trips.yml` descriptions, doc block, `mutable_zones` seed + snapshot |

```bash
git checkout ch4-materializations   # rewind to end of Chapter 4
git checkout main                   # latest (= end of Chapter 6)
```

## Setup

Prereqs: dbt-core 1.11 with `dbt-postgres` (see [Ch 2](https://github.com/hackyourfuture/datatrack/blob/main/Data%20Track/Week%2010/week_10__2_dbt_setup_azure.md) for install).

```bash
cp profiles.yml.example profiles.yml      # then edit `schema:` to dev_<your_name>
export PG_PASSWORD='your-week-6-password'  # macOS/Linux/WSL
just deps                                  # dbt deps  (only needed from v4 onward)
just build                                 # dbt build --select +fct_trips
```

`profiles.yml` is gitignored — it stays local so the file never lands in version control with a real password.

## Running from Airflow (Week 11)

[Week 11 Chapter 4](https://github.com/hackyourfuture/datatrack/blob/main/Data%20Track/Week%2011/week_11__4_sequential_pipelines.md) runs this dbt project end-to-end from an Airflow `BashOperator`. Three concrete setup steps on top of the local-dev setup above:

### 1. Copy the project into your Astro project's `include/` folder

Astro mounts `include/` into every container. Putting the dbt project under `include/dbt_project/` is the convention Ch4's `DBT_DIR` constant expects:

```bash
# from your Astro project directory
cp -r ../nyc-taxi-dbt-reference include/dbt_project

# or, if you cloned this repo as a submodule:
git submodule add git@github.com:hackyourfuture/nyc-taxi-dbt-reference include/dbt_project
cd include/dbt_project && git checkout week-11-airflow && cd ../..
```

### 2. Pass the connection credentials through `BashOperator.env`

The `profiles.yml.example` on this branch reads **four** env vars with sensible defaults for the shared class DB: `PG_HOST`, `PG_USER`, `PG_DBNAME`, `PG_SCHEMA`, plus `PG_PASSWORD` which has no default (security). Pull them from the `azure_pg` Airflow Connection you created in Ch4 and forward them to the bash command:

```python
DBT_DIR = "/usr/local/airflow/include/dbt_project"
DBT_ENV = {
    "PG_HOST": "{{ conn.azure_pg.host }}",
    "PG_USER": "{{ conn.azure_pg.login }}",
    "PG_PASSWORD": "{{ conn.azure_pg.password }}",
    "PG_DBNAME": "{{ conn.azure_pg.schema }}",
    "PG_SCHEMA": "airflow_taxi",   # team-shared schema, not per-student
}

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    env=DBT_ENV,
    append_env=True,   # inherit PATH etc. from the worker
)
dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=f"dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    env=DBT_ENV,
    append_env=True,
)
```

No passwords in `profiles.yml`, no passwords in DAG code, all credentials flow through the Airflow Connection the way the rest of Ch4 teaches.

### 3. Use a shared schema, not `dev_<your_name>`

Airflow writes into a single schema that downstream dashboards will read from. Pick one team-shared name (the example above uses `airflow_taxi`) and match it to whatever schema the `load_raw_trips` task writes to. A per-student `dev_<your_name>` schema defeats the point of orchestrating a shared pipeline.

### Differences from `main`

Only two deliberate differences from the `main` branch:

- `profiles.yml.example` reads `PG_HOST`, `PG_USER`, `PG_DBNAME`, `PG_SCHEMA` as env vars in addition to `PG_PASSWORD`, so the same file works for local dbt CLI (env vars fall back to the class defaults) and for Airflow (env vars come from the `azure_pg` connection).
- This README gained the Airflow section above.

All model code, tests, seeds, and snapshots are byte-identical to `main`. Students who finished Week 10 on `main` can switch to `week-11-airflow` without needing to redo any of the SQL work.

## CI

Every push runs `dbt build --select +fct_trips` against the shared Azure PostgreSQL instance. If the curriculum prose drifts away from runnable reality (test result counts change, columns get renamed, etc.) CI fails and the chapter gets fixed. See `.github/workflows/dbt-build.yml`.
