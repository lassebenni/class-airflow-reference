"""DAG integrity test for the shared class Airflow repo.

Runs on every PR via .github/workflows/integrity-ci.yml and locally via
``astro dev pytest tests/test_dag_integrity.py --args "-v"``. Catches
import errors in any student's DAG before the merge hits the shared
scheduler on the teacher VM.

Same test the curriculum teaches in Chapter 6 ("Testing DAGs"), applied
to the whole ``dags/`` tree rather than a single student's project.
"""

from airflow.models import DagBag


def test_no_import_errors():
    """Every .py in dags/ must import cleanly."""
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert dag_bag.import_errors == {}, (
        f"DAG import errors: {dag_bag.import_errors}"
    )


def test_every_dag_has_tags():
    """Light convention check so DAGs are discoverable in the UI's tag filter."""
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"DAG {dag_id} is missing tags"


def test_student_dags_are_namespaced():
    """Student DAGs under dags/<name>/ must prefix their dag_id with <name>.

    Enforces the Week 11 Ch8 convention that prevents thirty identical
    'taxi_pipeline' entries in the shared UI. Teacher DAGs at the top
    level of dags/ are exempt.
    """
    from pathlib import Path

    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    for dag_id, dag in dag_bag.dags.items():
        rel = Path(dag.fileloc).relative_to(Path(dag.fileloc).parents[2])
        parts = rel.parts
        if len(parts) >= 3 and parts[0] == "dags":
            student_dir = parts[1]
            assert dag_id.startswith(f"{student_dir}_"), (
                f"DAG {dag_id} lives under dags/{student_dir}/ but its dag_id "
                f"does not start with '{student_dir}_'. See Ch8 namespace convention."
            )
