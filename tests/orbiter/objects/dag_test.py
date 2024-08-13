from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.bash import OrbiterBashOperator
from orbiter.ast_helper import render_ast
from orbiter.objects.task_group import OrbiterTaskGroup

expected_basic_dag = """from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='dag_id', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):
    task_id_task = BashOperator(task_id='task_id', bash_command="echo 'hello world'")"""


expected_task_group = """from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from pendulum import DateTime, Timezone
with DAG(dag_id='dag_id', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False):
    with TaskGroup(group_id='foo') as foo:
        bar_task = BashOperator(task_id='bar', bash_command='bar')"""


def test_dag_render():
    # noinspection PyProtectedMember
    actual = render_ast(
        OrbiterDAG(dag_id="dag_id", file_path="")
        .add_tasks(
            OrbiterBashOperator(task_id="task_id", bash_command="echo 'hello world'")
        )
        ._to_ast()
    )
    assert actual == expected_basic_dag


def test_dag_render_task_group():
    # noinspection PyProtectedMember
    actual = render_ast(
        OrbiterDAG(dag_id="dag_id", file_path="")
        .add_tasks(
            OrbiterTaskGroup(
                task_group_id="foo",
                tasks=[OrbiterBashOperator(task_id="bar", bash_command="bar")],
            )
        )
        ._to_ast()
    )
    assert actual == expected_task_group
