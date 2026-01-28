from __future__ import annotations

from pathlib import Path

import pytest

from orbiter.default_translation import translate
from orbiter.file_types import FileTypeYAML
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.operators.empty import OrbiterEmptyOperator
from orbiter.objects.project import OrbiterProject
from orbiter.objects.task import OrbiterOperator, OrbiterTaskDependency
from orbiter.rules import dag_filter_rule, dag_rule, task_dependency_rule, task_filter_rule, task_rule
from orbiter.rules.rulesets import EMPTY_RULESET, TranslationConfig, TranslationRuleset


def test_loads(project_root):
    actual = TranslationRuleset(
        file_type={FileTypeYAML},
        dag_ruleset=EMPTY_RULESET,
        dag_filter_ruleset=EMPTY_RULESET,
        task_filter_ruleset=EMPTY_RULESET,
        task_ruleset=EMPTY_RULESET,
        task_dependency_ruleset=EMPTY_RULESET,
        post_processing_ruleset=EMPTY_RULESET,
    ).loads(project_root / "tests/resources/test_get_files_with_extension/one.YAML")
    assert actual == {"one": "foo"}


def test__get_files_with_extension(project_root):
    translation_ruleset = TranslationRuleset(
        file_type={FileTypeYAML},
        dag_ruleset=EMPTY_RULESET,
        dag_filter_ruleset=EMPTY_RULESET,
        task_filter_ruleset=EMPTY_RULESET,
        task_ruleset=EMPTY_RULESET,
        task_dependency_ruleset=EMPTY_RULESET,
        post_processing_ruleset=EMPTY_RULESET,
    )
    actual = translation_ruleset.get_files_with_extension(
        project_root / "tests/resources/test_get_files_with_extension"
    )
    expected = [
        (
            project_root / "tests/resources/test_get_files_with_extension/foo/bar/three.yaml",
            {"three": "baz"},
        ),
        (
            project_root / "tests/resources/test_get_files_with_extension/foo/bar/two.yml",
            {"two": "bar"},
        ),
        (
            project_root / "tests/resources/test_get_files_with_extension/one.YAML",
            {"one": "foo"},
        ),
    ]
    assert sorted(list(actual)) == sorted(expected)


@pytest.fixture(scope="function")
def test_ruleset_and_fake_path():
    fake_path = Path(__file__)

    def test_file_generator(_, __):
        yield (
            fake_path,
            {
                "dags": [
                    {
                        "dag_id": "dag_a",
                        "tasks": [
                            {"task_id": "task_a", "to": "task_b"},
                            {"task_id": "task_b"},
                        ],
                    }
                ]
            },
        )

    @dag_filter_rule
    def test_dag_filter_rule(val: dict) -> list[dict]:
        return val.get("dags", [])

    @dag_rule
    def test_dag_rule(val: dict) -> OrbiterDAG:
        return OrbiterDAG(dag_id=val["dag_id"], file_path=val["__file"])

    @task_filter_rule
    def test_task_filter_rule(val: dict) -> list[dict]:
        return val.get("tasks", [])

    @task_rule
    def test_task_rule(val: dict) -> OrbiterOperator | None:
        return OrbiterEmptyOperator(task_id=val.get("task_id"))

    @task_dependency_rule
    def test_task_dependency_rule(val: OrbiterDAG) -> list[OrbiterTaskDependency]:
        return [
            OrbiterTaskDependency(task_id=task["task_id"], downstream=to)
            for task in val.orbiter_kwargs["val"].get("tasks", [])
            if (to := task.get("to"))
        ]

    test_ruleset = TranslationRuleset(
        file_type={FileTypeYAML},
        dag_filter_ruleset={"ruleset": [test_dag_filter_rule]},
        dag_ruleset={"ruleset": [test_dag_rule]},
        task_filter_ruleset={"ruleset": [test_task_filter_rule]},
        task_ruleset={"ruleset": [test_task_rule]},
        task_dependency_ruleset={"ruleset": [test_task_dependency_rule]},
        post_processing_ruleset=EMPTY_RULESET,
        translate_fn=translate,
    )
    TranslationRuleset.get_files_with_extension = test_file_generator

    return test_ruleset, fake_path


def test_translate(test_ruleset_and_fake_path):
    (test_ruleset, fake_path) = test_ruleset_and_fake_path
    expected = OrbiterProject().add_dags(
        OrbiterDAG(dag_id="dag_a", file_path=fake_path).add_tasks(
            [OrbiterEmptyOperator(task_id="task_a").add_downstream("task_b"), OrbiterEmptyOperator(task_id="task_b")]
        )
    )

    actual = test_ruleset.translate_fn(translation_ruleset=test_ruleset, input_dir=fake_path)
    assert actual == expected
    assert actual.dags["dag_a"].tasks == expected.dags["dag_a"].tasks
    assert actual.dags["dag_a"].tasks["task_a"].downstream == expected.dags["dag_a"].tasks["task_a"].downstream


def test_translate_upfront(test_ruleset_and_fake_path):
    (test_ruleset, fake_path) = test_ruleset_and_fake_path
    test_ruleset.config = TranslationConfig(upfront=True)
    expected = OrbiterProject().add_dags(
        OrbiterDAG(dag_id="dag_a", file_path=fake_path).add_tasks(
            [OrbiterEmptyOperator(task_id="task_a").add_downstream("task_b"), OrbiterEmptyOperator(task_id="task_b")]
        )
    )

    actual = test_ruleset.translate_fn(translation_ruleset=test_ruleset, input_dir=fake_path)
    assert actual == expected
    assert actual.dags["dag_a"].tasks == expected.dags["dag_a"].tasks
    assert actual.dags["dag_a"].tasks["task_a"].downstream == expected.dags["dag_a"].tasks["task_a"].downstream


def test_translate_parallel(test_ruleset_and_fake_path):
    (test_ruleset, fake_path) = test_ruleset_and_fake_path
    test_ruleset.config = TranslationConfig(parallel=True)
    expected = OrbiterProject().add_dags(
        OrbiterDAG(dag_id="dag_a", file_path=fake_path).add_tasks(
            [OrbiterEmptyOperator(task_id="task_a").add_downstream("task_b"), OrbiterEmptyOperator(task_id="task_b")]
        )
    )

    actual = test_ruleset.translate_fn(translation_ruleset=test_ruleset, input_dir=fake_path)
    assert actual == expected
    assert actual.dags["dag_a"].tasks == expected.dags["dag_a"].tasks
    assert actual.dags["dag_a"].tasks["task_a"].downstream == expected.dags["dag_a"].tasks["task_a"].downstream
