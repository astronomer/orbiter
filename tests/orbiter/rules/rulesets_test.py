from orbiter import FileType
from orbiter.rules.rulesets import TranslationRuleset, EMPTY_RULESET


def test__get_files_with_extension(project_root):
    translation_ruleset = TranslationRuleset(
        file_type=FileType.YAML,
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
