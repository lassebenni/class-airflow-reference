# class-airflow-reference

The shared Airflow DAGs repo for the **HYF Data Track, Week 11**. Cloned onto the class VM by the bicep at [`datatrack/bicep/modules/vm-airflow.bicep`](https://github.com/lassebenni/datatrack) so that every merge to `main` deploys to the shared scheduler within ~60 seconds (a cron runs `git pull --ff-only` every minute).

Students follow the ten-step workflow in [`Data Track/Week 11/week_11__8_shared_airflow.md`](https://github.com/lassebenni/datatrack) to deploy their `taxi_pipeline` under their own subdirectory. See the chapter for the namespace + tag conventions; the short version is:

- Your DAG lives at `dags/<yourname>/<dag_file>.py`.
- `dag_id` is prefixed with your username (`lasse_taxi_pipeline`).
- `tags` includes `"student:<yourname>"` so the UI tag filter finds your DAGs.

## Repository layout

```text
class-airflow-reference/
├── dags/
│   ├── .gitkeep                       # intentionally empty; students add dags/<name>/
│   └── example_class_demo.py          # teacher-owned reference DAG so the scheduler always has something to parse
├── include/
│   └── dbt_project/                   # vendored copy of nyc-taxi-dbt-reference (week-11-airflow branch)
├── tests/
│   └── test_dag_integrity.py          # runs against the whole dags/ tree in CI before merge
├── requirements.txt                   # injected into the VM's Airflow image via _PIP_ADDITIONAL_REQUIREMENTS
└── .github/
    ├── CODEOWNERS                     # one student per subdirectory (add yourself on your first PR)
    └── workflows/integrity-ci.yml     # runs the integrity test on every PR
```

## How the deploy loop works

```text
your laptop                 GitHub (this repo)              teacher VM
───────────                 ──────────────────              ──────────
git push                  ▶ main branch              ◀───── cron: git pull (every 60s)
                                                     └───▶ dag-processor re-parses
                                                     └───▶ DAG appears in UI
```

No CI pipeline rebuilds a Docker image, no teacher intervention needed for a DAG change. The trade-off is a ~60-second deploy lag; in exchange the workflow is trivially debuggable ("`git pull` on my laptop matches what is in prod").

## Local development

Before opening a PR, validate your DAG locally with the same tools the VM uses:

```bash
# From the root of this repo, with Astro CLI installed
astro dev start
astro dev pytest tests/test_dag_integrity.py --args "-v"
```

The integrity test catches import errors before they reach the shared scheduler. A broken push does not crash the class VM (the separate `dag-processor` isolates parse failures), but it does show up as a red `1` badge on the DAGs list, so please run the test first.

## Shared-Airflow etiquette

Same as every shared production environment:

1. Pause and unpause only your own DAGs.
2. Trigger only your own DAGs.
3. Clear only your own task instances.
4. If something shared is broken (scheduler down, Postgres connection wrong), tell the teacher in the class channel. Do not "fix" it yourself from the UI.

Full rationale in [Ch8 of the curriculum](https://github.com/lassebenni/datatrack).
