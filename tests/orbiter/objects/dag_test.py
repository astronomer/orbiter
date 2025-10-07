from orbiter.objects.callbacks.smtp import OrbiterSmtpNotifierCallback
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.bash import OrbiterBashOperator
from orbiter.ast_helper import render_ast
from orbiter.objects.operators.empty import OrbiterEmptyOperator
from orbiter.objects.operators.sql import OrbiterSQLExecuteQueryOperator
from orbiter.objects.task_group import OrbiterTaskGroup

expected_basic_dag = """from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG(dag_id='dag_id'):
    task_id_task = BashOperator(task_id='task_id', bash_command="echo 'hello world'")"""


expected_task_group = """from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
with DAG(dag_id='dag_id'):
    with TaskGroup(group_id='foo') as foo:
        bar_task = BashOperator(task_id='bar', bash_command='bar')"""


def test_dag_render():
    # noinspection PyProtectedMember
    actual = render_ast(
        OrbiterDAG(dag_id="dag_id", file_path="")
        .add_tasks(OrbiterBashOperator(task_id="task_id", bash_command="echo 'hello world'"))
        ._to_ast()
    )
    assert actual == expected_basic_dag


def test_dag_render_task_group():
    # noinspection PyProtectedMember
    actual = render_ast(
        OrbiterDAG(dag_id="dag_id", file_path="")
        .add_tasks(
            OrbiterTaskGroup(task_group_id="foo").add_tasks(OrbiterBashOperator(task_id="bar", bash_command="bar"))
        )
        ._to_ast()
    )
    assert actual == expected_task_group


def test_dag_parse_complex():
    test = OrbiterDAG(dag_id="dag_id", file_path="").add_tasks(
        [
            OrbiterBashOperator(task_id="a", bash_command="a").add_downstream("b"),
            OrbiterTaskGroup(task_group_id="b")
            .add_tasks(
                [
                    OrbiterBashOperator(task_id="c", bash_command="c").add_downstream("d"),
                    OrbiterEmptyOperator(task_id="d"),
                ]
            )
            .add_downstream("e"),
            OrbiterSQLExecuteQueryOperator(task_id="e", sql="e", conn_id="foo"),
        ]
    )
    actual_json = test.model_dump_json()
    actual = OrbiterDAG.model_validate_json(actual_json)
    assert actual == test


def test_dag_parse_callback():
    test = OrbiterDAG(
        dag_id="dag_id",
        file_path="",
        on_failure_callback=OrbiterSmtpNotifierCallback(
            to="foo@test.com", from_email="bar@test.com", subject="Hello", html_content="World"
        ),
    )
    actual_json = test.model_dump_json()
    actual = OrbiterDAG.model_validate_json(actual_json)
    assert actual == test
