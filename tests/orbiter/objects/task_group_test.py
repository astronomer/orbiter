from pathlib import Path

from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.empty import OrbiterEmptyOperator
from orbiter.objects.task_group import OrbiterTaskGroup


def test_task_group_rendering():
    actual = OrbiterDAG(dag_id="dag", file_path=Path.cwd()).add_tasks(
        [
            OrbiterTaskGroup(task_group_id="foo").add_tasks([OrbiterEmptyOperator(task_id="a")]).add_downstream("bar"),
            OrbiterTaskGroup(task_group_id="bar").add_tasks([OrbiterEmptyOperator(task_id="b")]),
        ]
    )
    expected = r"""from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from pendulum import DateTime, Timezone
with DAG(dag_id='dag', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):
    with TaskGroup(group_id='foo') as foo:
        a_task = EmptyOperator(task_id='a')
    with TaskGroup(group_id='bar') as bar:
        b_task = EmptyOperator(task_id='b')
    foo >> bar"""  # noqa
    assert str(actual) == expected
