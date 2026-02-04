from orbiter.objects.asset import OrbiterAsset
from orbiter.objects.dag import OrbiterDAG

EXPECTED_ONE_DAG = """from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
with DAG(dag_id='foo', schedule=Asset('db://table')):
    empty_task = EmptyOperator(task_id='empty', doc_md='No tasks found... src=null')"""

EXPECTED_MANY_DAG = """from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
with DAG(dag_id='foo', schedule=[Asset('db://table1'), Asset('db://table2')]):
    empty_task = EmptyOperator(task_id='empty', doc_md='No tasks found... src=null')"""


def test_asset_one():
    actual_dag = OrbiterDAG(dag_id="foo", file_path="foo.py", schedule=OrbiterAsset(uri="db://table"))
    assert actual_dag.schedule.uri == "db://table"
    assert str(actual_dag) == EXPECTED_ONE_DAG


def test_asset_many():
    actual_dag = OrbiterDAG(
        dag_id="foo",
        file_path="foo.py",
        schedule=[
            OrbiterAsset(uri="db://table1"),
            OrbiterAsset(uri="db://table2"),
        ],
    )
    assert str(actual_dag) == EXPECTED_MANY_DAG
