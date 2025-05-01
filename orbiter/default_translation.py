from __future__ import annotations

import functools
import inspect
import re
import sys
import typing
from _operator import add
from collections import namedtuple
from itertools import chain
from pathlib import Path
from typing import Callable, List, Annotated, Union

import pathos
from pathos.helpers import freeze_support
from loguru import logger
from pydantic import AfterValidator, validate_call

from orbiter import import_from_qualname
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.project import OrbiterProject
from orbiter.objects.task import OrbiterOperator, OrbiterTaskDependency
from orbiter.objects.task_group import OrbiterTaskGroup
from orbiter.rules import trim_dict

if typing.TYPE_CHECKING:
    from orbiter.rules.rulesets import TranslationRuleset


def _backport_walk(input_dir: Path):
    """Path.walk() is only available in Python 3.12+, so, backport"""
    import os

    for result in os.walk(input_dir):
        yield Path(result[0]), result[1], result[2]


qualname_validator_regex = r"^[\w.]+$"
qualname_validator = re.compile(qualname_validator_regex)


def validate_translate_fn(
    translate_fn: str | Callable[["TranslationRuleset", Path], OrbiterProject],
) -> str | Callable[["TranslationRuleset", Path], OrbiterProject]:
    """
    ```pycon
    >>> validate_translate_fn(fake_translate) # a valid function works
    ... # doctest: +ELLIPSIS
    <function fake_translate at ...>
    >>> validate_translate_fn("orbiter.rules.rulesets.fake_translate") # a valid qualified name works
    ... # doctest: +ELLIPSIS
    <function fake_translate at ...>
    >>> import json
    >>> # noinspection PyTypeChecker
    ... validate_translate_fn(json.loads)  # an invalid function doesn't work
    ... # doctest: +IGNORE_EXCEPTION_DETAIL +ELLIPSIS
    Traceback (most recent call last):
    AssertionError: translate_fn=<function ...> must take two arguments
    >>> validate_translate_fn("???")  # an invalid entry doesn't work
    ... # doctest: +IGNORE_EXCEPTION_DETAIL +ELLIPSIS
    Traceback (most recent call last):
    AssertionError:

    ```
    """
    if isinstance(translate_fn, Callable):
        # unwrap function, if it's wrapped
        _translate_fn = inspect.unwrap(translate_fn)

        assert _translate_fn.__code__.co_argcount == 2, (
            f"translate_fn={_translate_fn} must take exactly two arguments: "
            "[translation_ruleset: TranslationRuleset, input_dir: Path]"
        )
    if isinstance(translate_fn, str):
        validate_qualified_imports([translate_fn])
        (_, _translate_fn) = import_from_qualname(translate_fn)
        translate_fn = _translate_fn
    return translate_fn


def validate_qualified_imports(qualified_imports: List[str]) -> List[str]:
    """
    ```pycon
    >>> validate_qualified_imports(["json", "package.module.Class"])
    ['json', 'package.module.Class']

    ```
    """
    for _qualname in qualified_imports:
        assert qualname_validator.match(_qualname), (
            f"Import Qualified Name='{_qualname}' is not valid."
            f"Qualified Names must match regex {qualname_validator_regex}"
        )
    return qualified_imports


QualifiedImport = Annotated[str, AfterValidator(validate_qualified_imports)]
TranslateFn = Annotated[
    Union[QualifiedImport, Callable[["TranslationRuleset", Path], OrbiterProject]],
    AfterValidator(validate_translate_fn),
]


def _apply_dag_filter_ruleset(
    relative_file: Path, input_dict: dict, translation_ruleset: "TranslationRuleset", file_log_prefix=""
):
    """Apply DAG FILTER Ruleset. Filter down to keys suspected of being translatable to a DAG, in priority order.
    Add __file DAG FILTER inputs and outputs, so it's available for both DAG and DAG FILTER rules
    """

    def with_file(d: dict) -> dict:
        try:
            __file_addition = {"__file": relative_file}
            return __file_addition | d
        except Exception as e:
            logger.opt(exception=e).debug(file_log_prefix + "Unable to add __file")
            return d

    dag_dicts: List[dict] = [
        with_file(dag_dict)
        for dag_dict in functools.reduce(
            add,
            translation_ruleset.dag_filter_ruleset.apply(val=with_file(input_dict)),
            [],
        )
    ]
    return dag_dicts


def _apply_dag_ruleset(relative_file: Path, dag_dict: dict, translation_ruleset: "TranslationRuleset"):
    """Apply DAG Ruleset. Convert the object to an `OrbiterDAG` via `dag_ruleset`,
    in priority-order, stopping when the first rule returns a match.
    """
    dag: OrbiterDAG | None = translation_ruleset.dag_ruleset.apply(
        val=dag_dict,
        take_first=True,
    )
    if dag:
        dag.orbiter_kwargs["file_path"] = relative_file
    return dag


def _apply_task_filter_ruleset(dag_dict, translation_ruleset):
    """TASK FILTER Ruleset - Many entries in dag_dict creates many task_dicts"""
    task_dicts = functools.reduce(
        add,
        translation_ruleset.task_filter_ruleset.apply(val=dag_dict),
        [],
    )
    return task_dicts


def _add_task_deduped(_task, _tasks, n="", dag_log_prefix=""):
    """
    If this task_id doesn't already exist, add it as normal to the tasks dictionary.
    If this task_id does exist - add a number to the end and try again

    ```pycon
    >>> from pydantic import BaseModel
    >>> class Task(BaseModel):
    ...   task_id: str
    >>> tasks = {}
    >>> _add_task_deduped(Task(task_id="foo"), tasks); tasks
    {'foo': Task(task_id='foo')}
    >>> _add_task_deduped(Task(task_id="foo"), tasks); tasks
    {'foo': Task(task_id='foo'), 'foo1': Task(task_id='foo1')}
    >>> _add_task_deduped(Task(task_id="foo"), tasks); tasks
    {'foo': Task(task_id='foo'), 'foo1': Task(task_id='foo1'), 'foo2': Task(task_id='foo2')}

    ```
    """
    if hasattr(_task, "task_id"):
        _id = "task_id"
    elif hasattr(_task, "task_group_id"):
        _id = "task_group_id"
    else:
        raise TypeError("Attempting to add a task without a `task_id` or `task_group_id` attribute")

    old_task_id = getattr(_task, _id)
    new_task_id = old_task_id + n
    if new_task_id not in _tasks:
        if n != "":
            logger.warning(
                dag_log_prefix + f"{old_task_id} encountered more than once, task IDs must be unique! "
                f"Modifying task ID to '{new_task_id}'!"
            )
        setattr(_task, _id, new_task_id)
        _tasks[new_task_id] = _task
    else:
        try:
            n = str(int(n) + 1)
        except ValueError:
            n = "1"
        _add_task_deduped(_task, _tasks, n)


def _apply_task_ruleset(tasks, task_dict, translation_ruleset, dag_log_prefix=""):
    """Apply TASK Ruleset - one rule produces one task"""
    task: OrbiterOperator = translation_ruleset.task_ruleset.apply(val=task_dict, take_first=True)
    if task is None:
        logger.warning(
            dag_log_prefix + f"Couldn't extract task from task_dict={trim_dict(task_dict)} "
            f"with task_ruleset={translation_ruleset.task_ruleset}"
        )
    else:
        _add_task_deduped(task, tasks)


def _get_parent_for_task_dependency(
    task_dependency: OrbiterTaskDependency, this: OrbiterDAG | OrbiterTaskGroup
) -> OrbiterDAG | OrbiterTaskGroup | None:
    """Look through any children in the `.tasks` property for a matching task_id, recursing into anything that contains
    `.tasks`. Return the parent object that contains the task_id, or None if it's not found.

    ```pycon
    >>> from orbiter.objects.operators.empty import OrbiterEmptyOperator
    >>> _get_parent_for_task_dependency(
    ...     OrbiterTaskDependency(task_id="bar", downstream="baz"),
    ...     OrbiterDAG(dag_id="foo", file_path='', tasks={"bar": OrbiterEmptyOperator(task_id="bar")})
    ... ).dag_id  # returns the dag
    'foo'
    >>> _get_parent_for_task_dependency(
    ...     OrbiterTaskDependency(task_id="bar", downstream="qux"),
    ...     OrbiterDAG(dag_id="foo", file_path='', tasks={
    ...         "bar": OrbiterTaskGroup(task_group_id="bar", tasks={"bop": OrbiterEmptyOperator(task_id="bop")})
    ...     })
    ... ).dag_id  # returns the parent dag, even if a task group is the target
    'foo'
    >>> _get_parent_for_task_dependency(
    ...     OrbiterTaskDependency(task_id="baz", downstream="qux"),
    ...     OrbiterDAG(dag_id="foo", file_path='', tasks={
    ...         "bar": OrbiterTaskGroup(task_group_id="bar").add_tasks(OrbiterEmptyOperator(task_id="baz"))
    ...     })
    ... ).task_group_id  # returns a child task group, if it contains the task
    'bar'
    >>> _get_parent_for_task_dependency(
    ...     OrbiterTaskDependency(task_id="bonk", downstream="end"),
    ...     OrbiterTaskGroup(task_group_id="foo").add_tasks([
    ...         OrbiterTaskGroup(task_group_id="bar").add_tasks(OrbiterEmptyOperator(task_id="baz")),
    ...         OrbiterTaskGroup(task_group_id="qux").add_tasks(OrbiterEmptyOperator(task_id="bonk"))
    ...     ])
    ... ).task_group_id  # returns a nested task group that contains the task
    'qux'
    >>> _get_parent_for_task_dependency(
    ...     OrbiterTaskDependency(task_id="qux", downstream="qop"),
    ...     OrbiterDAG(dag_id="foo", file_path='', tasks={
    ...         "bar": OrbiterTaskGroup(task_group_id="bar").add_tasks(OrbiterEmptyOperator(task_id="baz"))
    ...     })
    ... ) # returns nothing if the task was never found

    ```
    """
    for task in getattr(this, "tasks", {}).values():
        found = None
        if getattr(task, "task_id", "") == task_dependency.task_id:
            found = this
        elif isinstance(task, OrbiterTaskGroup):
            if _found := _get_parent_for_task_dependency(task_dependency, task):
                found = _found
        if found:
            return found
    return None


def _apply_task_dependency_ruleset(
    dag: OrbiterDAG, dag_dict: dict, translation_ruleset: TranslationRuleset, dag_log_prefix=""
):
    """Apply TASK DEPENDENCY Ruleset @ Dag-level.
    Sets 'downstream' on Tasks, based on the task_dependency_ruleset, modifying them in-place.
    """
    task_dependencies: List[OrbiterTaskDependency] = (
        list(chain(*translation_ruleset.task_dependency_ruleset.apply(val=dag))) or []
    )
    if not len(task_dependencies):
        logger.warning(dag_log_prefix + f"Couldn't find task dependencies in dag={trim_dict(dag_dict)}")
    for task_dependency in task_dependencies:
        task_dependency: OrbiterTaskDependency
        if parent := _get_parent_for_task_dependency(task_dependency, dag):
            parent.tasks[task_dependency.task_id].add_downstream(task_dependency)
        else:
            logger.warning(dag_log_prefix + f"Couldn't find task_id={task_dependency.task_id} in tasks")
            continue


def _apply_post_processing_ruleset(project, translation_ruleset):
    """Apply POST PROCESSING Ruleset. Modify project in-place"""
    translation_ruleset.post_processing_ruleset.apply(val=project, take_first=False)


CandidateInput = namedtuple("CandidateInput", ["i", "file", "input_dict", "input_dir", "translation_ruleset"])
DagDictsToProcess = namedtuple("DagDictToProcess", ["file", "index", "dag_dicts"])


def _filter_dag_candidates_in_file(candidate_input: CandidateInput) -> DagDictsToProcess:
    """Run internally in the `translate` function - to multiprocess finding DAG candidates in files"""
    i, file, input_dict, input_dir, translation_ruleset = candidate_input
    i += 1
    try:
        relative_file = file.relative_to(input_dir.parent)
        file_log_prefix = f"[File {i}={relative_file}] "
    except ValueError:
        relative_file = file.resolve()
        file_log_prefix = f"[File {i}={relative_file}] "
        logger.debug(file_log_prefix + "File is not relative to input_dir")

    logger.debug(file_log_prefix + "Filtering candidates via DAG Filter Ruleset")
    dag_dicts = _apply_dag_filter_ruleset(relative_file, input_dict, translation_ruleset, file_log_prefix)
    logger.debug(file_log_prefix + f"Found {len(dag_dicts)} DAG candidates")

    return DagDictsToProcess(file=relative_file, index=i, dag_dicts=dag_dicts)


@validate_call
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
    from orbiter.rules.rulesets import TranslationRuleset

    # validate translation_ruleset is of correct type
    if not isinstance(translation_ruleset, TranslationRuleset):
        raise RuntimeError(
            f"Error! type(translation_ruleset)=={type(translation_ruleset)}!=TranslationRuleset! Exiting!"
        )

    # resolve any . or .. or ~ in the input_dir
    input_dir = input_dir.resolve()

    # Create an initial OrbiterProject
    project = OrbiterProject()

    logger.info("Finding all files with the expected file type")
    files = enumerate(translation_ruleset.get_files_with_extension(input_dir))

    Pool = (  # noqa
        # pyinstaller/exe can't do multiproc
        pathos.multiprocessing.ThreadPool if getattr(sys, "frozen", False) else pathos.multiprocessing.ProcessingPool
    )
    with Pool() as pool:
        logger.info("Filtering candidates from all files in parallel")
        dag_candidates_all_files = pool.map(
            _filter_dag_candidates_in_file,
            (CandidateInput(i, file, input_dict, input_dir, translation_ruleset) for i, (file, input_dict) in files),
        )

    # this could be parallel, as well, but doesn't really need to be?
    logger.info("Translating DAGs and Tasks from candidates in all files")
    for relative_file, i, dag_dicts in dag_candidates_all_files:
        file_log_prefix = f"[File {i}={relative_file}] "
        logger.debug(file_log_prefix + "Translating DAGs and Tasks from candidates")

        for dag_dict in dag_dicts:
            logger.debug(file_log_prefix + "Applying DAG Ruleset to translate candidate to DAG")
            dag = _apply_dag_ruleset(relative_file, dag_dict, translation_ruleset)
            if dag is None:
                logger.warning(
                    file_log_prefix
                    + f"Couldn't translate DAG from dag_dict={dag_dict} "
                    + f"with dag_ruleset={translation_ruleset.dag_ruleset}"
                )
                continue

            dag_log_prefix = file_log_prefix[:-1] + f"[DAG={dag.dag_id}] "
            logger.debug(dag_log_prefix + "Filtering candidates from DAG via Task Filter Ruleset")
            task_dicts = _apply_task_filter_ruleset(dag_dict, translation_ruleset)
            logger.debug(dag_log_prefix + f"Found {len(task_dicts)} Task candidates")

            tasks = {}
            for task_dict in task_dicts:
                logger.debug(dag_log_prefix + "Applying Task Ruleset to translate candidate to Task")
                _apply_task_ruleset(tasks, task_dict, translation_ruleset)

            logger.debug(dag_log_prefix + f"Adding {len(tasks)} tasks to DAG")
            dag.add_tasks(tasks.values())

            _apply_task_dependency_ruleset(dag, dag_dict, translation_ruleset, dag_log_prefix)

            logger.debug(dag_log_prefix + "Adding to Project")
            project.add_dags(dag)

    _apply_post_processing_ruleset(project, translation_ruleset)
    return project


def fake_translate(translation_ruleset: "TranslationRuleset", input_dir: Path) -> OrbiterProject:
    """Fake translate function, for testing"""
    _ = (translation_ruleset, input_dir)
    return OrbiterProject()


if __name__ == "__main__":
    freeze_support()
