from __future__ import annotations

import functools
import typing
from _operator import add
from itertools import chain
from pathlib import Path
from typing import List

from loguru import logger
from pydantic import validate_call

from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.project import OrbiterProject
from orbiter.objects.task import OrbiterTaskDependency, OrbiterOperator
from orbiter.objects.task_group import OrbiterTaskGroup
from orbiter.rules import trim_dict

if typing.TYPE_CHECKING:
    from orbiter.rules.rulesets import TranslationRuleset


def _add_task_deduped(_task, _tasks, n=""):
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
                f"{old_task_id} encountered more than once, task IDs must be unique! "
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

    if not isinstance(translation_ruleset, TranslationRuleset):
        raise RuntimeError(
            f"Error! type(translation_ruleset)=={type(translation_ruleset)}!=TranslationRuleset! Exiting!"
        )

    # Create an initial OrbiterProject
    project = OrbiterProject()

    for i, (file, input_dict) in enumerate(translation_ruleset.get_files_with_extension(input_dir)):
        logger.info(f"Translating [File {i}]={file.resolve()}")

        # DAG FILTER Ruleset - filter down to keys suspected of being translatable to a DAG, in priority order.
        # Add __file DAG FILTER inputs and outputs, so it's available for both DAG and DAG FILTER rules
        def with_file(d: dict) -> dict:
            try:
                __file_addition = {"__file": (input_dir / file.relative_to(input_dir))}
                return __file_addition | d
            except Exception as e:
                logger.opt(exception=e).debug("Unable to add __file")
                return d

        dag_dicts: List[dict] = [
            with_file(dag_dict)
            for dag_dict in functools.reduce(
                add,
                translation_ruleset.dag_filter_ruleset.apply(val=with_file(input_dict)),
                [],
            )
        ]
        logger.debug(f"Found {len(dag_dicts)} DAG candidates in {file.resolve()}")
        for dag_dict in dag_dicts:
            # DAG Ruleset - convert the object to an `OrbiterDAG` via `dag_ruleset`,
            #         in priority-order, stopping when the first rule returns a match
            dag: OrbiterDAG | None = translation_ruleset.dag_ruleset.apply(
                val=dag_dict,
                take_first=True,
            )
            if dag is None:
                logger.warning(
                    f"Couldn't extract DAG from dag_dict={dag_dict} with dag_ruleset={translation_ruleset.dag_ruleset}"
                )
                continue
            dag.orbiter_kwargs["file_path"] = file.relative_to(input_dir)

            tasks = {}
            # TASK FILTER Ruleset - Many entries in dag_dict -> Many task_dict
            task_dicts = functools.reduce(
                add,
                translation_ruleset.task_filter_ruleset.apply(val=dag_dict),
                [],
            )
            logger.debug(f"Found {len(task_dicts)} Task candidates in {dag.dag_id} in {file.resolve()}")
            for task_dict in task_dicts:
                # TASK Ruleset one -> one
                task: OrbiterOperator = translation_ruleset.task_ruleset.apply(val=task_dict, take_first=True)
                if task is None:
                    logger.warning(f"Couldn't extract task from expected task_dict={task_dict}")
                    continue

                _add_task_deduped(task, tasks)
            logger.debug(f"Adding {len(tasks)} tasks to DAG {dag.dag_id}")
            dag.add_tasks(tasks.values())

            # Dag-Level TASK DEPENDENCY Ruleset
            task_dependencies: List[OrbiterTaskDependency] = (
                list(chain(*translation_ruleset.task_dependency_ruleset.apply(val=dag))) or []
            )
            if not len(task_dependencies):
                logger.warning(f"Couldn't find task dependencies in dag={trim_dict(dag_dict)}")
            for task_dependency in task_dependencies:
                task_dependency: OrbiterTaskDependency
                if parent := _get_parent_for_task_dependency(task_dependency, dag):
                    parent.tasks[task_dependency.task_id].add_downstream(task_dependency)
                else:
                    logger.warning(f"Couldn't find task_id={task_dependency.task_id} in tasks for dag_id={dag.dag_id}")
                    continue

            logger.debug(f"Adding DAG {dag.dag_id} to project")
            project.add_dags(dag)

    # POST PROCESSING Ruleset
    translation_ruleset.post_processing_ruleset.apply(val=project, take_first=False)

    return project


def fake_translate(translation_ruleset: TranslationRuleset, input_dir: Path) -> OrbiterProject:
    """Fake translate function, for testing"""
    _ = (translation_ruleset, input_dir)
    return OrbiterProject()
