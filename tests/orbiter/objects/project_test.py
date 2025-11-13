from pathlib import Path

import pytest
import yaml

from orbiter.meta import OrbiterMeta
from orbiter.objects.operators.unmapped import OrbiterUnmappedOperator
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.connection import OrbiterConnection
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.env_var import OrbiterEnvVar
from orbiter.objects.pool import OrbiterPool
from orbiter.objects.project import OrbiterProject
from orbiter.objects.task import OrbiterTask, OrbiterTaskDependency
from orbiter.objects.timetables.multiple_cron_trigger_timetable import OrbiterMultipleCronTriggerTimetable
from orbiter.objects.variable import OrbiterVariable


@pytest.fixture(scope="function")
def project():
    return OrbiterProject().add_dags(
        dags=[
            OrbiterDAG(
                dag_id="foo",
                file_path="foo.py",
                schedule=OrbiterMultipleCronTriggerTimetable(crons=["0 1 * * *", "*/5 0 * * *"]),
                doc_md="foo",
                orbiter_meta=OrbiterMeta(
                    matched_rule_source="src",
                    matched_rule_name="rule",
                    matched_rule_params_doc={"a": "b"},
                    matched_rule_docstring="docstring",
                    matched_rule_priority=1,
                    visited_keys=["foo"],
                ),
            ).add_tasks(
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
                        orbiter_meta=OrbiterMeta(
                            matched_rule_source="src",
                            matched_rule_name="rule",
                            matched_rule_params_doc={"a": "b"},
                            matched_rule_docstring="docstring",
                            matched_rule_priority=1,
                            visited_keys=["foo"],
                        ),
                        imports=[
                            OrbiterRequirement(
                                package="apache-airflow",
                                module="airflow.operators.empty",
                                names=["EmptyOperator"],
                            ),
                            OrbiterRequirement(sys_package="git"),
                        ],
                    ).add_downstream(
                        OrbiterTaskDependency(task_id="foo", downstream="bar"),
                    ),
                    OrbiterUnmappedOperator(task_id="bar", source="foo"),
                ]
            )
        ]
    )


def test_project_serde(project):
    # Test serialization
    actual_json = project.model_dump_json()
    actual_reserialized = OrbiterProject.model_validate_json(actual_json, strict=True)
    assert actual_reserialized == project


def test_project_render(tmpdir, project):
    tmpdir = Path(tmpdir)

    # Test render
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
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from include.unmapped import UnmappedOperator
with DAG(dag_id='foo', schedule=MultipleCronTriggerTimetable('0 1 * * *', '*/5 0 * * *', timezone='UTC'), doc_md='foo'):
    foo_task = EmptyOperator(task_id='foo', doc='some other thing')
    bar_task = UnmappedOperator(task_id='bar', source='foo')
    foo_task >> bar_task"""
    assert actual_dag == expected_dag

    actual_include = tmpdir / "include/unmapped.py"
    assert actual_include.exists(), actual_include
    actual_include = actual_include.read_text()
    assert "class UnmappedOperator(EmptyOperator):" in actual_include
