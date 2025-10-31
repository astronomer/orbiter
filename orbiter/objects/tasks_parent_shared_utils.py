from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

from loguru import logger

if TYPE_CHECKING:
    from orbiter.objects.dag import OrbiterDAG
    from orbiter.objects.task import OrbiterTaskDependency, OrbiterOperator
    from orbiter.objects.task_group import OrbiterTaskGroup


def _get_task_dependency_parent(
    self: "OrbiterDAG | OrbiterTaskGroup",
    task_dependency: "OrbiterTaskDependency",
) -> "OrbiterDAG | OrbiterTaskGroup | None":
    """Look through any children in the `.tasks` property for a matching task_id, recursing into anything that contains
    `.tasks`. Return the parent object that contains the task_id, or None if it's not found.

    ```pycon
    >>> from orbiter.objects.operators.empty import OrbiterEmptyOperator
    >>> from orbiter.objects.dag import OrbiterDAG
    >>> from orbiter.objects.task import OrbiterTaskDependency
    >>> from orbiter.objects.task_group import OrbiterTaskGroup
    >>> OrbiterDAG(dag_id="foo", file_path='', tasks={"bar": OrbiterEmptyOperator(task_id="bar")}).get_task_dependency_parent(
    ...     OrbiterTaskDependency(task_id="bar", downstream="baz"),
    ... ).dag_id  # returns the dag
    'foo'
    >>> OrbiterDAG(dag_id="foo", file_path='', tasks={
    ...     "bar": OrbiterTaskGroup(task_group_id="bar", tasks={"bop": OrbiterEmptyOperator(task_id="bop")})
    ... }).get_task_dependency_parent(
    ...     OrbiterTaskDependency(task_id="bar", downstream="qux"),
    ... ).dag_id  # returns the parent dag, even if a task group is the target
    'foo'
    >>> OrbiterDAG(dag_id="foo", file_path='', tasks={
    ...    "bar": OrbiterTaskGroup(task_group_id="bar").add_tasks(OrbiterEmptyOperator(task_id="baz"))
    ... }).get_task_dependency_parent(
    ...     OrbiterTaskDependency(task_id="baz", downstream="qux"),
    ... ).task_group_id  # returns a child task group, if it contains the task
    'bar'
    >>> OrbiterTaskGroup(task_group_id="foo").add_tasks([
    ...     OrbiterTaskGroup(task_group_id="bar").add_tasks(OrbiterEmptyOperator(task_id="baz")),
    ...     OrbiterTaskGroup(task_group_id="qux").add_tasks(OrbiterEmptyOperator(task_id="bonk"))
    ... ]).get_task_dependency_parent(
    ...     OrbiterTaskDependency(task_id="bonk", downstream="end"),
    ... ).task_group_id  # returns a nested task group that contains the task
    'qux'
    >>> OrbiterDAG(dag_id="foo", file_path='', tasks={
    ...     "bar": OrbiterTaskGroup(task_group_id="bar").add_tasks(OrbiterEmptyOperator(task_id="baz"))
    ... }).get_task_dependency_parent(
    ...     OrbiterTaskDependency(task_id="qux", downstream="qop"),
    ... ) # returns nothing if the task was never found

    ```
    """
    from orbiter.objects.task_group import OrbiterTaskGroup
    from orbiter.objects.task import OrbiterTaskDependency

    task_id = task_dependency.task_id if isinstance(task_dependency, OrbiterTaskDependency) else task_dependency

    for task in getattr(self, "tasks", {}).values():
        found = None
        if getattr(task, "task_id", "") == task_id:
            found = self
        elif isinstance(task, OrbiterTaskGroup):
            if _found := _get_task_dependency_parent(task, task_dependency):
                found = _found
        if found:
            return found
    return None


def _add_tasks(
    self,
    tasks: (OrbiterOperator | OrbiterTaskGroup | Iterable[OrbiterOperator | OrbiterTaskGroup]),
) -> "OrbiterDAG | OrbiterTaskGroup":
    """Add one or more [`OrbiterOperators`][orbiter.objects.task.OrbiterOperator] to the DAG or Task Group

    If the task_id doesn't already exist, in the DAG, it is added to the `tasks` dictionary.
    If this task_id does exist, it is suffixed with a number to the end to prevent duplicate task_ids.

    ```pycon
    >>> from orbiter.objects.dag import OrbiterDAG
    >>> from orbiter.objects.operators.empty import OrbiterEmptyOperator
    >>> OrbiterDAG(file_path="", dag_id="foo").add_tasks(OrbiterEmptyOperator(task_id="bar")).tasks
    {'bar': bar_task = EmptyOperator(task_id='bar')}
    >>> OrbiterDAG(file_path="", dag_id="foo").add_tasks([OrbiterEmptyOperator(task_id="bar")]).tasks
    {'bar': bar_task = EmptyOperator(task_id='bar')}
    >>> OrbiterDAG(file_path="", dag_id="foo").add_tasks([
    ...   OrbiterEmptyOperator(task_id="foo"),
    ...   OrbiterEmptyOperator(task_id="foo"),
    ...   OrbiterEmptyOperator(task_id="foo"),
    ... ]).tasks
    {'foo': foo_task = EmptyOperator(task_id='foo'), 'foo_1': foo_1_task = EmptyOperator(task_id='foo_1'), 'foo_2': foo_2_task = EmptyOperator(task_id='foo_2')}

    ```

    !!! tip

        Validation requires a `OrbiterTaskGroup`, `OrbiterOperator` (or subclass), or list of either to be passed
        ```pycon
        >>> from orbiter.objects.dag import OrbiterDAG
        >>> # noinspection PyTypeChecker
        ... OrbiterDAG(file_path="", dag_id="foo").add_tasks("bar")
        ... # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        AttributeError: ...
        >>> # noinspection PyTypeChecker
        ... OrbiterDAG(file_path="", dag_id="foo").add_tasks(["bar"])
        ... # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        AttributeError: ...

        ```
    :param tasks: List of [OrbiterOperator][orbiter.objects.task.OrbiterOperator], or OrbiterTaskGroup or subclass
    :type tasks: OrbiterOperator | OrbiterTaskGroup | Iterable[OrbiterOperator | OrbiterTaskGroup]
    :return: self
    :rtype: OrbiterProject
    """
    from orbiter.objects.task import OrbiterOperator
    from orbiter.objects.task_group import OrbiterTaskGroup

    if (
        isinstance(tasks, OrbiterOperator)
        or isinstance(tasks, OrbiterTaskGroup)
        or issubclass(type(tasks), OrbiterOperator)
    ):
        tasks = [tasks]

    for task in tasks:
        # Deduplicate task_id - add a number to the end, if it already exists in the DAG
        i = 0
        while (task_id := task.task_id if i == 0 else f"{task.task_id}_{i}") in self.tasks:
            i += 1
        if i > 0:
            logger.warning(
                f"{task.task_id} encountered more than once, task IDs must be unique! Modifying task ID to '{task_id}'!"
            )
            setattr(task, "task_group_id" if hasattr(task, "task_group_id") else "task_id", task_id)

        self.tasks[task.task_id] = task
    return self
