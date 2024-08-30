from orbiter.file_types import FileTypeYAML
from orbiter.rules.rulesets import TranslationRuleset, EMPTY_RULESET


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
            project_root
            / "tests/resources/test_get_files_with_extension/foo/bar/three.yaml",
            {"three": "baz"},
        ),
        (
            project_root
            / "tests/resources/test_get_files_with_extension/foo/bar/two.yml",
            {"two": "bar"},
        ),
        (
            project_root / "tests/resources/test_get_files_with_extension/one.YAML",
            {"one": "foo"},
        ),
    ]
    assert sorted(list(actual)) == sorted(expected)
