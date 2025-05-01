from __future__ import annotations

import functools
import typing
from _operator import add
from itertools import chain
from pathlib import Path
from typing import List

from loguru import logger

from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.project import OrbiterProject
from orbiter.objects.task import OrbiterTaskDependency, OrbiterOperator
from orbiter.rules import trim_dict

if typing.TYPE_CHECKING:
    from orbiter.rules.rulesets import TranslationRuleset
    from orbiter.objects.task_group import OrbiterTaskGroup


def apply_dag_filter_ruleset(
    input_dict: dict,
    translation_ruleset: TranslationRuleset,
    file_relative_to_input_dir_parent: Path,
    file_log_prefix: str = "",
) -> list[dict]:
    """Apply all rules from [DAG Filter Ruleset][orbiter.rules.ruleset.DAGFilterRuleset] to filter down to keys
    that look like they can be translated to a DAG, in priority order.

    !!! note

        The file name is added under a `__file` key to both input and output dictionaries, for use in future rules.

    :param input_dict: The input dictionary to filter, e.g. the file that was loaded and converted into a python dict
    :param translation_ruleset: The translation ruleset to use
    :param file_relative_to_input_dir_parent: The file relative to the input directory's parent (is added under '__file' key)
    :param file_log_prefix: The file log prefix to use for logging, default is empty
    :return: A list of dictionaries that look like they can be translated to a DAG
    """

    def with_file(d: dict) -> dict:
        try:
            __file_addition = {"__file": file_relative_to_input_dir_parent}
            return __file_addition | d
        except Exception as e:
            logger.opt(exception=e).debug(f"{file_log_prefix} Unable to add __file")
            return d

    return [
        with_file(dag_dict)
        for dag_dict in functools.reduce(
            add,
            translation_ruleset.dag_filter_ruleset.apply(val=with_file(input_dict)),
            [],
        )
    ]


def apply_dag_ruleset(
    dag_dict, translation_ruleset, file_relative_to_input_dir_parent, file_log_prefix=""
) -> OrbiterDAG:
    """Apply all rules from [DAG Ruleset][orbiter.rules.ruleset.DAGRuleset] to convert the object to an `OrbiterDAG`,
    in priority order, stopping when the first rule returns a match.
    If no rule returns a match, the entry is filtered.

    !!! note
        The file name is added to `orbiter_kwargs` under a `file_path` key, for use in future rules.

    :param dag_dict: DAG Candidate, filtered via @dag_filter_rules
    :param translation_ruleset: The translation ruleset to use
    :param file_relative_to_input_dir_parent: The file (relative to the input directory's parent)
    :param file_log_prefix: The file log prefix to use for logging, default is empty
    :return: An `OrbiterDAG` object
    """
    dag: OrbiterDAG | None = translation_ruleset.dag_ruleset.apply(
        val=dag_dict,
        take_first=True,
    )
    if dag is None:
        raise RuntimeError(
            f"{file_log_prefix} Couldn't extract DAG from dag_dict={dag_dict} with dag_ruleset={translation_ruleset.dag_ruleset}"
        )
    dag.orbiter_kwargs["file_path"] = file_relative_to_input_dir_parent
    return dag


def apply_task_filter_ruleset(dag_dict: dict, translation_ruleset: TranslationRuleset) -> list[dict]:
    """Apply all rules from [Task Filter Ruleset][orbiter.rules.ruleset.TaskFilterRuleset] to filter down to keys
    that look like they can be translated to a Task, in priority order.

    Many entries in the dag_dict result in many task_dicts.

    :param dag_dict: The dict to filter - the same dict that was passed to the DAG ruleset
    :param translation_ruleset: The translation ruleset to use
    :return: A list of dictionaries that look like they can be translated to a Task
    """
    return functools.reduce(
        add,
        translation_ruleset.task_filter_ruleset.apply(val=dag_dict),
        [],
    )


def apply_task_ruleset(task_dict: dict, translation_ruleset: TranslationRuleset) -> OrbiterOperator | OrbiterTaskGroup:
    """Apply all rules from [Task Ruleset][orbiter.rules.ruleset.TaskRuleset] to convert the object to a Task,
    in priority order, stopping when the first rule returns a match.

    One task_dict makes up to one task (unless it produces a TaskGroup, which can contain many tasks).
    If no rule returns a match, the entry is filtered.

    :param task_dict: A dict to translate - what was returned from the Task Filter ruleset
    :param translation_ruleset: The translation ruleset to use
    :return: An `OrbiterOperator` (or descendant) or `OrbiterTaskGroup`
    """
    return translation_ruleset.task_ruleset.apply(val=task_dict, take_first=True)


def apply_task_dependency_ruleset(
    dag: OrbiterDAG, dag_dict: dict, translation_ruleset: TranslationRuleset, dag_log_prefix: str = ""
) -> None:
    """Apply all rules from [Task Dependency Ruleset][orbiter.rules.ruleset.TaskDependencyRuleset] to create a list of
    [`OrbiterTaskDependency`][orbiter.objects.task.OrbiterTaskDependency],
    which are then added to each task in the [`OrbiterDAG`][orbiter.objects.dag.OrbiterDAG] in-place.

    :param dag: The DAG to add the task dependencies to
    :param dag_dict: The dict to filter - the same dict that was passed to the DAG ruleset
    :param dag_log_prefix: The file log prefix to use for logging, default is empty
    :param translation_ruleset: The translation ruleset to use
    :return: None
    """
    task_dependencies: List[OrbiterTaskDependency] = (
        list(chain(*translation_ruleset.task_dependency_ruleset.apply(val=dag))) or []
    )
    if not len(task_dependencies):
        logger.warning(f"{dag_log_prefix} Couldn't find task dependencies in dag={trim_dict(dag_dict)}")
    for task_dependency in task_dependencies:
        task_dependency: OrbiterTaskDependency
        if parent := dag.get_task_dependency_parent(task_dependency):
            parent.tasks[task_dependency.task_id].add_downstream(task_dependency)
        else:
            logger.warning(f"{dag_log_prefix} Couldn't find task_id={task_dependency.task_id} in tasks")
            continue


def apply_post_processing_ruleset(project: OrbiterProject, translation_ruleset: TranslationRuleset) -> None:
    """Apply all rules from [Post Processing Ruleset][orbiter.rules.ruleset.PostProcessingRuleset]
    to modify project in-place

    :param project: The OrbiterProject to modify
    :param translation_ruleset: The translation ruleset to use
    :return: None
    """
    translation_ruleset.post_processing_ruleset.apply(val=project, take_first=False)


def validate_translate_function_inputs(translation_ruleset: TranslationRuleset, input_dir: Path) -> None:
    """Validate the inputs to the translation function

    !!! note

        ```pycon
        >>> from orbiter.rules.rulesets import EMPTY_TRANSLATION_RULESET
        >>> validate_translate_function_inputs(None, Path())  # noqa
        Traceback (most recent call last):
        TypeError: Error! type(translation_ruleset)==<class 'NoneType'>!=TranslationRuleset! Exiting!
        >>> validate_translate_function_inputs(EMPTY_TRANSLATION_RULESET, '.')  # noqa
        Traceback (most recent call last):
        TypeError: Error! type(input_dir)==<class 'str'>!=Path! Exiting!

        ```

    :raises RuntimeError: if input is an invalid type
    """
    from orbiter.rules.rulesets import TranslationRuleset

    if not isinstance(translation_ruleset, TranslationRuleset):
        raise TypeError(f"Error! type(translation_ruleset)=={type(translation_ruleset)}!=TranslationRuleset! Exiting!")
    if not isinstance(input_dir, Path):
        raise TypeError(f"Error! type(input_dir)=={type(input_dir)}!=Path! Exiting!")


def translate(translation_ruleset, input_dir: Path) -> OrbiterProject:
    """
    Orbiter expects a folder containing text files which may have a structure like:
    ```json
    {"<workflow name>": { ...<workflow properties>, "<task name>": { ...<task properties>} }}
    ```

    The standard translation function performs the following steps:

    ![Diagram of Orbiter Translation](../orbiter_diagram.png)

    1. [**Find all files**][orbiter.rules.rulesets.TranslationRuleset.get_files_with_extension] with the expected
        [`TranslationRuleset.file_type`][orbiter.rules.rulesets.TranslationRuleset]
        (`.json`, `.xml`, `.yaml`, etc.) in the input folder.
        - [**Load each file**][orbiter.rules.rulesets.TranslationRuleset.loads] and turn it into a Python Dictionary.
    2. **For each file:** Apply the [`TranslationRuleset.dag_filter_ruleset`][orbiter.rules.rulesets.DAGFilterRuleset]
        to filter down to entries that can translate to a DAG, in priority order.
        The file name is added under a `__file` key to both input and output dictionaries for the DAG Filter rule.
        - **For each dictionary**: Apply the [`TranslationRuleset.dag_ruleset`][orbiter.rules.rulesets.DAGRuleset],
        to convert the object to an [`OrbiterDAG`][orbiter.objects.dag.OrbiterDAG],
        in priority-order, stopping when the first rule returns a match.
        If no rule returns a match, the entry is filtered.
    3. Apply the [`TranslationRuleset.task_filter_ruleset`][orbiter.rules.rulesets.TaskFilterRuleset]
        to filter down to entries in the DAG that can translate to a Task, in priority-order.
        - **For each:** Apply the [`TranslationRuleset.task_ruleset`][orbiter.rules.rulesets.TaskRuleset],
            to convert the object to a specific Task, in priority-order, stopping when the first rule returns a match.
            If no rule returns a match, the entry is filtered.
    4. After the DAG and Tasks are mapped, the
        [`TranslationRuleset.task_dependency_ruleset`][orbiter.rules.rulesets.TaskDependencyRuleset]
        is applied in priority-order, stopping when the first rule returns a match,
        to create a list of
        [`OrbiterTaskDependency`][orbiter.objects.task.OrbiterTaskDependency],
        which are then added to each task in the
        [`OrbiterDAG`][orbiter.objects.dag.OrbiterDAG]
    5. Apply the [`TranslationRuleset.post_processing_ruleset`][orbiter.rules.rulesets.PostProcessingRuleset],
        against the [`OrbiterProject`][orbiter.objects.project.OrbiterProject], which can make modifications after all
        other rules have been applied.
    6. After translation - the [`OrbiterProject`][orbiter.objects.project.OrbiterProject]
        is rendered to the output folder.
    """
    validate_translate_function_inputs(translation_ruleset, input_dir)

    logger.debug("Creating an empty OrbiterProject")
    project = OrbiterProject()

    for i, (file, input_dict) in enumerate(translation_ruleset.get_files_with_extension(input_dir)):
        file = file.resolve()
        file_relative_to_input_dir_parent = file.relative_to(input_dir.parent)
        file_log_prefix = f"[File {i}={file_relative_to_input_dir_parent}]"
        logger.info(f"{file_log_prefix} Translating file")

        logger.debug(f"{file_log_prefix} Extracting DAG candidates")
        dag_dicts = apply_dag_filter_ruleset(
            input_dict, translation_ruleset, file_relative_to_input_dir_parent, file_log_prefix
        )
        logger.debug(f"{file_log_prefix} Found {len(dag_dicts)} DAG candidates")

        for dag_dict in dag_dicts:
            logger.debug(f"{file_log_prefix} Translating DAG Candidate to DAG")
            try:
                dag = apply_dag_ruleset(
                    dag_dict,
                    translation_ruleset,
                    file_relative_to_input_dir_parent,
                    file_log_prefix,
                )
            except RuntimeError as e:
                logger.opt(exception=e).warning(e)
                continue

            dag_log_prefix = f"{file_log_prefix}[DAG={dag.dag_id}]"

            logger.debug(f"{dag_log_prefix} Extracting Task candidates")
            task_dicts = apply_task_filter_ruleset(dag_dict, translation_ruleset)
            logger.debug(f"{dag_log_prefix} Found {len(task_dicts)} Task candidates")

            logger.debug(f"{dag_log_prefix} Translating Task Candidates to Tasks")
            tasks = []
            for task_dict in task_dicts:
                task = apply_task_ruleset(task_dict, translation_ruleset)
                if task is None:
                    logger.warning(f"{dag_log_prefix} Couldn't extract task from expected task_dict={task_dict}")
                    continue
                tasks.append(task)
            logger.debug(f"{dag_log_prefix} Adding {len(tasks)} tasks")
            dag.add_tasks(tasks)

            logger.debug(f"{dag_log_prefix} Extracting Task Dependencies to apply to Tasks")
            apply_task_dependency_ruleset(dag, dag_dict, translation_ruleset, dag_log_prefix)

            logger.debug(f"{dag_log_prefix} Adding DAG {dag.dag_id} to project")
            project.add_dags(dag)

    apply_post_processing_ruleset(project, translation_ruleset)

    return project


def fake_translate(translation_ruleset: TranslationRuleset, input_dir: Path) -> OrbiterProject:
    """Fake translate function, for testing"""
    _ = (translation_ruleset, input_dir)
    return OrbiterProject()


if __name__ == "__main__":
    import doctest

    doctest.testmod(optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
