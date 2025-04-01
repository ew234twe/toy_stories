import pytest
from pathlib import Path
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dagbag():
    dag_folder = Path(__file__).parent.parent / "dags"
    dagbag = DagBag(dag_folder=dag_folder, include_examples=False)
    return dagbag
