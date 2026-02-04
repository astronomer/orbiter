from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.dataset import OrbiterDataset

EXPECTED_ONE_DAG = """from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
with DAG(dag_id='foo', schedule=Dataset('db://table')):
    empty_task = EmptyOperator(task_id='empty', doc_md='No tasks found... src=null')"""

EXPECTED_MANY_DAG = """from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
with DAG(dag_id='foo', schedule=[Dataset('db://table1'), Dataset('db://table2')]):
    empty_task = EmptyOperator(task_id='empty', doc_md='No tasks found... src=null')"""


def test_dataset_one():
    actual_dag = OrbiterDAG(dag_id="foo", file_path="foo.py", schedule=OrbiterDataset(uri="db://table"))
    assert actual_dag.schedule.uri == "db://table"
    assert str(actual_dag) == EXPECTED_ONE_DAG


def test_dataset_many():
    actual_dag = OrbiterDAG(
        dag_id="foo",
        file_path="foo.py",
        schedule=[
            OrbiterDataset(uri="db://table1"),
            OrbiterDataset(uri="db://table2"),
        ],
    )
    assert str(actual_dag) == EXPECTED_MANY_DAG
