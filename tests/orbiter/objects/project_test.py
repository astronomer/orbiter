from pathlib import Path

import yaml

from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.connection import OrbiterConnection
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.env_var import OrbiterEnvVar
from orbiter.objects.pool import OrbiterPool
from orbiter.objects.project import OrbiterProject
from orbiter.objects.task import OrbiterTask
from orbiter.objects.variable import OrbiterVariable


def test_project_render(tmpdir):
    tmpdir = Path(tmpdir)
    # noinspection PyArgumentList
    project = OrbiterProject().add_dags(
        dags=[
            OrbiterDAG(dag_id="foo", file_path="foo.py", schedule=None, doc_md="foo").add_tasks(
                tasks=[
                    OrbiterTask(
                        task_id="foo",
                        doc="some other thing",
                        pool="foo",
                        pool_slots=1,
                        trigger_rule="one_success",
                        orbiter_pool=OrbiterPool(name="foo", description="foo", slots=1),
                        orbiter_vars={OrbiterVariable(key="foo", value="bar")},
                        orbiter_env_vars={OrbiterEnvVar(key="foo", value="bar")},
                        orbiter_conns={OrbiterConnection(conn_id="foo", host="bar", password="baz")},
                        orbiter_kwargs={"SOME_INPUT": "FOOBAR"},
                        imports=[
                            OrbiterRequirement(
                                package="apache-airflow",
                                module="airflow.operators.empty",
                                names=["EmptyOperator"],
                            ),
                            OrbiterRequirement(sys_package="git"),
                        ],
                    )
                ]
            )
        ]
    )
    project.render(tmpdir)

    actual_requirements = (tmpdir / "requirements.txt").read_text()
    expected_requirements = "apache-airflow\npendulum"
    assert actual_requirements == expected_requirements

    actual_packages = (tmpdir / "packages.txt").read_text()
    expected_packages = "git"
    assert actual_packages == expected_packages

    actual_airflow_settings = (tmpdir / "airflow_settings.yaml").read_text()
    expected_airflow_settings = yaml.dump(
        {
            "airflow": {
                "connections": [
                    {
                        "conn_host": "bar",
                        "conn_id": "foo",
                        "conn_password": "baz",  # pragma: allowlist secret
                        "conn_type": "generic",
                    }
                ],
                "pools": [{"pool_description": "foo", "pool_name": "foo", "pool_slot": 1}],
                "variables": [{"variable_name": "foo", "variable_value": "bar"}],
            }
        }
    )
    assert actual_airflow_settings == expected_airflow_settings

    actual_dag = tmpdir / "dags/foo.py"
    assert actual_dag.exists(), actual_dag
    actual_dag = actual_dag.read_text()
    expected_dag = """from airflow import DAG
from airflow.operators.empty import EmptyOperator
from pendulum import DateTime, Timezone
with DAG(dag_id='foo', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False, doc_md='foo'):
    foo_task = EmptyOperator(task_id='foo', doc='some other thing')"""
    assert actual_dag == expected_dag
