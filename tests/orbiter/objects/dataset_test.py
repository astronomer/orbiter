from orbiter.ast_helper import render_ast
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.dataset import OrbiterDataset


def test_dataset_repr():
    dataset = OrbiterDataset(uri="s3://bucket/key")
    assert str(dataset) == "Dataset('s3://bucket/key')"


def test_dag_schedule_with_single_dataset():
    dag = OrbiterDAG(
        dag_id="foo",
        file_path="foo.py",
        schedule=OrbiterDataset(uri="db://table"),
    )

    # noinspection PyProtectedMember
    actual = render_ast(dag._to_ast())
    expected = """from airflow import DAG
from airflow.datasets import Dataset
with DAG(dag_id='foo', schedule=Dataset('db://table')):"""
    assert actual == expected


def test_dag_schedule_with_multiple_datasets():
    dag = OrbiterDAG(
        dag_id="foo",
        file_path="foo.py",
        schedule=[
            OrbiterDataset(uri="db://table1"),
            OrbiterDataset(uri="db://table2"),
        ],
    )

    # noinspection PyProtectedMember
    actual = render_ast(dag._to_ast())
    # Note: list of OrbiterDataset objects will be rendered via their repr
    expected = """from airflow import DAG
from airflow.datasets import Dataset
with DAG(dag_id='foo', schedule=[Dataset('db://table1'), Dataset('db://table2')]):"""
    assert actual == expected
