from __future__ import annotations

import functools
import inspect
import re
from _operator import add
from abc import ABC
from itertools import chain
from pathlib import Path
from typing import List, Any, Collection, Annotated, Callable, Union

from loguru import logger
from pydantic import BaseModel, AfterValidator, validate_call

from orbiter import FileType, import_from_qualname, load_filetype
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.project import OrbiterProject
from orbiter.objects.task import OrbiterOperator, OrbiterTaskDependency
from orbiter.objects.task_group import OrbiterTaskGroup
from orbiter.rules import (
    Rule,
    DAGFilterRule,
    DAGRule,
    TaskFilterRule,
    TaskRule,
    TaskDependencyRule,
    PostProcessingRule,
    EMPTY_RULE,
)  # noqa: F401

qualname_validator_regex = r"^[\w.]+$"
qualname_validator = re.compile(qualname_validator_regex)


def validate_translate_fn(
    translate_fn: str | Callable[["TranslationRuleset", Path], OrbiterProject]
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


# noinspection t
@validate_call
def translate(translation_ruleset, input_dir: Path) -> OrbiterProject:
    """
    Orbiter, by default, expects a folder containing text files (`.json`, `.xml`, `.yaml`, etc.)
    which may have a structure like:
    ```json
    {"<workflow name>": { ...<workflow properties>, "<task name>": { ...<task properties>} }}
    ```

    The default translation function (`orbiter.rules.rulesets.translate`) performs the following steps:

    1. Look in the input folder for all files with the expected
    [`TranslationRuleset.file_type`][orbiter.rules.rulesets.TranslationRuleset].
        For each file, it will:
        1. Load the file and turn it into a Python Dictionary
        2. Apply the [`TranslationRuleset.dag_filter_ruleset`][orbiter.rules.rulesets.DAGFilterRuleset]
            to filter down to keys suspected of being translatable to a DAG,
            in priority order. For each suspected DAG dict:
            1. Apply the [`TranslationRuleset.dag_ruleset`][orbiter.rules.rulesets.DAGRuleset],
                to convert the object to an [`OrbiterDAG`][orbiter.objects.dag.OrbiterDAG],
                in priority-order, stopping when the first rule returns a match.
            2. Apply the [`TranslationRuleset.task_filter_ruleset`][orbiter.rules.rulesets.TaskFilterRuleset]
                to filter down to keys suspected of being translatable to a Task,
                in priority-order. For each suspected Task dict:
                1. Apply the [`TranslationRuleset.task_ruleset`][orbiter.rules.rulesets.TaskRuleset],
                    in priority-order, stopping when the first rule returns a match,
                    to convert the dictionary to a specific type of Task. If no rule returns a match,
                    the dict is filtered.
            3. After the DAG and Tasks are mapped, the
                [`TranslationRuleset.task_dependency_ruleset`][orbiter.rules.rulesets.TaskDependencyRuleset]
                is applied in priority-order, stopping when the first rule returns a match,
                to create a list of
                [`OrbiterTaskDependency`][orbiter.objects.task.OrbiterTaskDependency]
                These task dependencies are then added to each of the tasks they apply to in the
                [`OrbiterDAG`][orbiter.objects.dag.OrbiterDAG]
    2. Apply the `post_processing_ruleset` against the
        [`OrbiterProject`][orbiter.objects.project.OrbiterProject]
    3. Return the [`OrbiterDAG`][orbiter.objects.project.OrbiterProject]
    """
    if not isinstance(translation_ruleset, TranslationRuleset):
        raise RuntimeError(
            f"Error! type(translation_ruleset)=={type(translation_ruleset)}!=TranslationRuleset! Exiting!"
        )

    # Create an initial OrbiterProject
    project = OrbiterProject()

    extension = translation_ruleset.file_type.value.lower()

    logger.info(f"Finding files with extension={extension} in {input_dir}")
    files = [
        directory / file
        for (directory, _, files) in input_dir.walk()
        for file in files
        if extension in file
    ]
    logger.info(f"Found {len(files)} files with extension={extension} in {input_dir}")

    for file in files:
        logger.info(f"Translating file={file.resolve()}")

        # Load the file and convert it into a python dict
        input_dict = load_filetype(file.read_text(), translation_ruleset.file_type)

        # DAG FILTER Ruleset - filter down to keys suspected of being translatable to a DAG, in priority order.
        dag_dicts = functools.reduce(
            add,
            translation_ruleset.dag_filter_ruleset.apply(val=input_dict),
            [],
        )
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
            dag.orbiter_kwargs["file_path"] = str(file.resolve())

            tasks = {}
            # TASK FILTER Ruleset - Many entries in dag_dict -> Many task_dict
            task_dicts = functools.reduce(
                add,
                translation_ruleset.task_filter_ruleset.apply(val=dag_dict),
                [],
            )
            logger.debug(
                f"Found {len(task_dicts)} Task candidates in {dag.dag_id} in {file.resolve()}"
            )
            for task_dict in task_dicts:
                # TASK Ruleset one -> one
                task: OrbiterOperator = translation_ruleset.task_ruleset.apply(
                    val=task_dict, take_first=True
                )
                if task is None:
                    logger.warning(
                        f"Couldn't extract task from expected task_dict={task_dict}"
                    )
                    continue

                _add_task_deduped(task, tasks)
            logger.debug(f"Adding {len(tasks)} tasks to DAG {dag.dag_id}")
            dag.add_tasks(tasks.values())

            # Dag-Level TASK DEPENDENCY Ruleset
            task_dependencies: List[OrbiterTaskDependency] = (
                list(chain(*translation_ruleset.task_dependency_ruleset.apply(val=dag)))
                or []
            )
            if not len(task_dependencies):
                logger.warning(f"Couldn't find task dependencies in dag={dag_dict}")
            for task_dependency in task_dependencies:
                task_dependency: OrbiterTaskDependency
                if task_dependency.task_id not in dag.tasks:
                    logger.warning(
                        f"Couldn't find task_id={task_dependency.task_id} in tasks={tasks} for dag_id={dag.dag_id}"
                    )
                    continue
                else:
                    dag.tasks[task_dependency.task_id].add_downstream(task_dependency)

            logger.debug(f"Adding DAG {dag.dag_id} to project")
            project.add_dags(dag)

    # POST PROCESSING Ruleset
    translation_ruleset.post_processing_ruleset.apply(val=project, take_first=False)

    return project


def fake_translate(
    translation_ruleset: TranslationRuleset, input_dir: Path
) -> OrbiterProject:
    """Fake translate function, for testing"""
    _ = (translation_ruleset, input_dir)
    return OrbiterProject()


class Ruleset(BaseModel, frozen=True, extra="forbid"):
    """A list of rules, which are evaluated to generate different types of output

    You must pass a [`Rule`][orbiter.rules.Rule] (or `dict` with the schema of [`Rule`][orbiter.rules.Rule])
    ```pycon
    >>> from orbiter.rules import rule
    >>> @rule
    ... def x(val):
    ...    return None
    >>> Ruleset(ruleset=[x, {"rule": lambda: None}])
    ... # doctest: +ELLIPSIS
    Ruleset(ruleset=[Rule(...), Rule(...)])

    ```

    !!! note

        You can't pass non-Rules
        ```pycon
        >>> # noinspection PyTypeChecker
        ... Ruleset(ruleset=[None])
        ... # doctest: +ELLIPSIS
        Traceback (most recent call last):
        pydantic_core._pydantic_core.ValidationError: ...

        ```

    :param ruleset: List of [`Rule`][orbiter.rules.Rule] (or `dict` with the schema of [`Rule`][orbiter.rules.Rule])
    :type ruleset: List[Rule | Callable[[Any], Any | None]]
    """

    ruleset: List[Rule | Callable[[dict | Any], Any | None]]

    def apply_many(
        self,
        input_val: Collection[Any],
        take_first: bool = False,
    ) -> List[List[Any]] | List[Any]:
        """
        Apply a ruleset to each item in collection (such as `dict().items()`)
        and return any results that are not `None`

        You can turn the output of `apply_many` into a dict, if the rule takes and returns a tuple
        ```pycon
        >>> from itertools import chain
        >>> from orbiter.rules import rule

        >>> @rule
        ... def filter_for_type_folder(val):
        ...   (key, val) = val
        ...   return (key, val) if val.get('Type', '') == 'Folder' else None
        >>> ruleset = Ruleset(ruleset=[filter_for_type_folder])
        >>> input_dict = {
        ...    "a": {"Type": "Folder"},
        ...    "b": {"Type": "File"},
        ...    "c": {"Type": "Folder"},
        ... }
        >>> dict(chain(*chain(ruleset.apply_many(input_dict.items()))))
        ... # use dict(chain(*chain(...))), if using `take_first=True`, to turn many results back into dict
        {'a': {'Type': 'Folder'}, 'c': {'Type': 'Folder'}}
        >>> dict(ruleset.apply_many(input_dict.items(), take_first=True))
        ... # use dict(...) directly, if using `take_first=True`, to turn results back into dict
        {'a': {'Type': 'Folder'}, 'c': {'Type': 'Folder'}}

        ```

        You cannot pass input without length
        ```pycon
        >>> ruleset.apply_many({})
        ... # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        RuntimeError: Input is not Collection[Any] with length!

        ```
        :param input_val: List to evaluate ruleset over
        :type input_val: Collection[Any]
        :param take_first: Only take the first (if any) result from each ruleset application
        :type take_first: bool
        :returns: List of list with all non-null evaluations for each item<br>
                  or list of the first non-null evaluation for each item (if `take_first=True`)
        :rtype: List[List[Any]] | List[Any]
        :raises RuntimeError: if the Ruleset or input_vals are empty
        :raises RuntimeError: if the Rule raises an exception
        """
        # Validate Input
        if not input_val or not len(input_val):
            raise RuntimeError("Input is not `Collection[Any]` with length!")

        return [
            results[0] if take_first else results
            for item in input_val
            if (results := self.apply(take_first=False, val=item)) is not None
            and len(results)
        ]

    def _sorted(self) -> List[Rule]:
        """Return a copy of the ruleset, sorted by priority
        ```pycon
        >>> sorted_rules = Ruleset(ruleset=[
        ...   Rule(rule=lambda x: 1, priority=1),
        ...   Rule(rule=lambda x: 99, priority=99)]
        ... )._sorted()
        >>> sorted_rules[0].priority
        99
        >>> sorted_rules[-1].priority
        1

        ```
        """
        return sorted(self.ruleset, key=lambda r: r.priority, reverse=True)

    @validate_call
    def apply(self, take_first: bool = False, **kwargs) -> List[Any] | Any:
        """
        Apply all rules in ruleset **to a single item**, in priority order, removing any `None` results.

        A ruleset with one rule can produce **up to one** result
        ```pycon
        >>> from orbiter.rules import rule

        >>> @rule
        ... def gt_4(val):
        ...     return str(val) if val > 4 else None
        >>> Ruleset(ruleset=[gt_4]).apply(val=5)
        ['5']

        ```

        Many rules can produce many results, one for each rule.
        ```pycon
        >>> @rule
        ... def gt_3(val):
        ...    return str(val) if val > 3 else None
        >>> Ruleset(ruleset=[gt_4, gt_3]).apply(val=5)
        ['5', '5']

        ```

        The `take_first` flag will evaluate rules in the ruleset and return the first match
        ```pycon
        >>> Ruleset(ruleset=[gt_4, gt_3]).apply(val=5, take_first=True)
        '5'

        ```

        If nothing matched, an empty list is returned
        ```pycon
        >>> @rule
        ... def always_none(val):
        ...     return None
        >>> @rule
        ... def more_always_none(val):
        ...     return None
        >>> Ruleset(ruleset=[always_none, more_always_none]).apply(val=5)
        []

        ```

        If nothing matched, and `take_first=True`, `None` is returned
        ```pycon
        >>> Ruleset(ruleset=[always_none, more_always_none]).apply(val=5, take_first=True)
        ... # None

        ```

        !!! tip

            If no input is given, an error is returned
            ```pycon
            >>> Ruleset(ruleset=[always_none]).apply()
            Traceback (most recent call last):
            RuntimeError: No values provided! Supply at least one key=val pair as kwargs!

            ```

        :param take_first: only take the first (if any) result from the ruleset application
        :type take_first: bool
        :param kwargs: key=val pairs to pass to the evaluated rule function
        :type kwargs: Any
        :returns: List of rules that evaluated to `Any` (in priority order),
                    or an empty list,
                    or `Any` (if `take_first=True`)
        :rtype: List[Any] | Any | None
        :raises RuntimeError: if the Ruleset is empty or input_val is None
        :raises RuntimeError: if the Rule raises an exception
        """
        if not len(kwargs):
            raise RuntimeError(
                "No values provided! Supply at least one key=val pair as kwargs!"
            )
        results = []
        for _rule in self._sorted():
            result = _rule(**kwargs)
            if result is not None:
                logger.debug(
                    "---------\n"
                    f"Ruleset: '{self.__class__.__name__}' Matched\n"
                    f"Rule: '{_rule.__name__}' Matched\n"
                    f"INPUT: {kwargs}\n"
                    f"RETURN: {result}\n"
                    f"---------"
                )
                results.append(result)
                if take_first:
                    return result
        return None if take_first and not len(results) else results


class DAGFilterRuleset(Ruleset):
    """Ruleset of [`DAGFilterRule`][orbiter.rules.DAGFilterRule]"""

    ruleset: List[
        DAGFilterRule | Rule | Callable[[dict], Collection[dict] | None] | dict
    ]


class DAGRuleset(Ruleset):
    """Ruleset of [`DAGRule`][orbiter.rules.DAGRule]"""

    ruleset: List[DAGRule | Rule | Callable[[dict], OrbiterDAG | None] | dict]


class TaskFilterRuleset(Ruleset):
    """Ruleset of [`TaskFilterRule`][orbiter.rules.TaskFilterRule]"""

    ruleset: List[
        TaskFilterRule | Rule | Callable[[dict], Collection[dict] | None] | dict
    ]


class TaskRuleset(Ruleset):
    """Ruleset of [`TaskRule`][orbiter.rules.TaskRule]"""

    ruleset: List[
        TaskRule
        | Rule
        | Callable[[dict], OrbiterOperator | OrbiterTaskGroup | None]
        | dict
    ]


class TaskDependencyRuleset(Ruleset):
    """Ruleset of [`TaskDependencyRule`][orbiter.rules.TaskDependencyRule]"""

    ruleset: List[
        TaskDependencyRule
        | Rule
        | Callable[[OrbiterDAG], List[OrbiterTaskDependency] | None]
        | dict
    ]


class PostProcessingRuleset(Ruleset):
    """Ruleset of [`PostProcessingRule`][orbiter.rules.PostProcessingRule]"""

    ruleset: List[PostProcessingRule | Rule | Callable[[OrbiterProject], None] | dict]


class TranslationRuleset(BaseModel, ABC, extra="forbid"):
    """A container for [`Ruleset`][orbiter.rules.rulesets.Ruleset], which applies to a given type of translation

    ```pycon
    >>> TranslationRuleset(
    ...   file_type=FileType.JSON,                                      # Has a file type
    ...   translate_fn=fake_translate,                                  # and can have a callable
    ...   # translate_fn="orbiter.rules.translate.fake_translate",      # or a qualified name to a function
    ...   dag_filter_ruleset={"ruleset": [{"rule": lambda x: None}]},   # Rulesets can be dict within dicts
    ...   dag_ruleset=DAGRuleset(ruleset=[Rule(rule=lambda x: None)]),  # or objects within objects
    ...   task_filter_ruleset=EMPTY_RULESET,                            # or a mix
    ...   task_ruleset=EMPTY_RULESET,
    ...   task_dependency_ruleset=EMPTY_RULESET,                        # Omitted for brevity
    ...   post_processing_ruleset=EMPTY_RULESET,
    ... )
    TranslationRuleset(...)

    ```

    :param file_type: FileType to translate (`.json`, `.xml`, `.yaml`, etc.)
    :type file_type: FileType
    :param dag_filter_ruleset: [`DAGFilterRuleset`][orbiter.rules.rulesets.DAGFilterRuleset]
        (of [`DAGFilterRule`][orbiter.rules.DAGFilterRule])
    :type dag_filter_ruleset: DAGFilterRuleset | dict
    :param dag_ruleset: [`DAGRuleset`][orbiter.rules.rulesets.DAGRuleset] (of [`DAGRules`][orbiter.rules.DAGRule])
    :type dag_ruleset: DAGRuleset | dict
    :param task_filter_ruleset: [`TaskFilterRule`][orbiter.rules.rulesets.TaskFilterRule]
        (of [`TaskFilterRule`][orbiter.rules.TaskFilterRule])
    :type task_filter_ruleset: TaskFilterRuleset | dict
    :param task_ruleset: [`TaskRuleset`][orbiter.rules.rulesets.TaskRuleset] (of [`TaskRules`][orbiter.rules.TaskRule])
    :type task_ruleset: TaskRuleset | dict
    :param task_dependency_ruleset: [`TaskDependencyRuleset`][orbiter.rules.rulesets.TaskDependencyRuleset]
        (of [`TaskDependencyRules`][orbiter.rules.TaskDependencyRule])
    :type task_dependency_ruleset: TaskDependencyRuleset | dict
    :param post_processing_ruleset: [`PostProcessingRuleset`][orbiter.rules.rulesets.PostProcessingRuleset]
        (of [`PostProcessingRules`][orbiter.rules.PostProcessingRule])
    :type post_processing_ruleset: PostProcessingRuleset | dict
    :param translate_fn: Either a qualified name to a function (e.g. `path.to.file.function`), or a function reference,
        with the signature: <br>
        `(`[`translation_ruleset: Translation Ruleset`][orbiter.rules.rulesets.TranslationRuleset]`, input_dir: Path) -> `
        [`OrbiterProject`][orbiter.objects.project.OrbiterProject]
    :type translate_fn: Callable[[TranslationRuleset, Path], OrbiterProject] | str | TranslateFn
    """  # noqa: E501

    file_type: FileType
    dag_filter_ruleset: DAGFilterRuleset | dict
    dag_ruleset: DAGRuleset | dict
    task_filter_ruleset: TaskFilterRuleset | dict
    task_ruleset: TaskRuleset | dict
    task_dependency_ruleset: TaskDependencyRuleset | dict
    post_processing_ruleset: PostProcessingRuleset | dict
    translate_fn: TranslateFn = translate


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
    new_task_id = _task.task_id + n
    if new_task_id not in _tasks:
        if n != "":
            logger.warning(
                f"{_task.task_id} encountered more than once, task IDs must be unique! "
                f"Modifying task ID to '{new_task_id}'!"
            )
        _task.task_id = new_task_id
        _tasks[new_task_id] = _task
    else:
        try:
            n = str(int(n) + 1)
        except ValueError:
            n = "1"
        _add_task_deduped(_task, _tasks, n)


EMPTY_RULESET = {"ruleset": [EMPTY_RULE]}
"""Empty ruleset, for testing"""

if __name__ == "__main__":
    import doctest

    doctest.testmod(
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL
    )
