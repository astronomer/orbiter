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


def test_dag_render_dynamic_mapping():
    from orbiter.objects.operators.python import OrbiterPythonOperator
    from orbiter.objects.dag import OrbiterDAG
    from orbiter.ast_helper import render_ast
    from orbiter.objects.task import OrbiterDynamicTaskMapping

    def one_two_three_traditional():
        # this adjustment is due to op_args expecting each argument as a list
        return [[1], [2], [3]]

    def plus_10_traditional(x):
        return x + 10

    one_two_three_task = OrbiterPythonOperator(task_id="one_two_three_task", python_callable=one_two_three_traditional)

    plus_10_task = OrbiterPythonOperator(
        task_id="plus_10_task",
        python_callable=plus_10_traditional
    )

    plus_10_task_dtm = OrbiterDynamicTaskMapping(
        operator=plus_10_task,
        partial_kwargs={"task_id": "plus_10_task"},
        expand_kwargs={"op_args": one_two_three_task.output},
    )

    dag = OrbiterDAG(dag_id="dynamic_mapping_classic_etl", file_path="").add_tasks([one_two_three_task, plus_10_task_dtm])

    actual = render_ast(dag._to_ast())
    assert (
        "one_two_three_task = PythonOperator(task_id='one_two_three_task', python_callable=one_two_three_traditional)"
        in actual
    )
    assert (
        "plus_10_task = PythonOperator.partial(task_id='plus_10_task', python_callable=plus_10_traditional).expand(op_args=one_two_three_task.output)"
        in actual
    )


def test_dag_render_dynamic_mapping_list():
    from orbiter.objects.operators.python import OrbiterPythonOperator
    from orbiter.objects.dag import OrbiterDAG
    from orbiter.ast_helper import render_ast
    from orbiter.objects.task import OrbiterDynamicTaskMapping

    def plus_10_traditional(x):
        return x + 10

    plus_10_task = OrbiterPythonOperator(
        task_id="plus_10_task",
        python_callable=plus_10_traditional
    )

    plus_10_task_dtm = OrbiterDynamicTaskMapping(
        operator=plus_10_task,
        partial_kwargs={"task_id": "plus_10_task"},
        expand_kwargs={"op_args": [[1], [2], [3]]},
    )

    dag = OrbiterDAG(dag_id="dynamic_mapping_list_etl", file_path="").add_tasks([plus_10_task_dtm])

    actual = render_ast(dag._to_ast())
    assert (
        "plus_10_task = PythonOperator.partial(task_id='plus_10_task', python_callable=plus_10_traditional).expand(op_args=[[1], [2], [3]])"
        in actual
    )
