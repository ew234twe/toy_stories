from airflow.models import DagBag


def test_all_dags_work(dagbag: DagBag):
    assert dagbag.import_errors == {}


def test_hello_world_dag(dagbag: DagBag):
    dag = dagbag.get_dag(dag_id="hello_world")
    assert dag


def test_simple_etl_dag(dagbag: DagBag):
    dag = dagbag.get_dag(dag_id="simple_etl")
    assert dag
    result = dag.test()
    assert result.state
