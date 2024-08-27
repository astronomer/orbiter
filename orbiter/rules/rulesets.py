from __future__ import annotations

import functools
import inspect
import re
import uuid
from _operator import add
from abc import ABC
from itertools import chain
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Any, Collection, Annotated, Callable, Union, Generator

from loguru import logger
from pydantic import BaseModel, AfterValidator, validate_call

from orbiter import FileType
from orbiter import import_from_qualname
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
def xmltodict_parse(input_str: str) -> Any:
    """Calls `xmltodict.parse` and does post-processing fixes.

    !!! note

        The original [`xmltodict.parse`](https://pypi.org/project/xmltodict/) method returns EITHER:

        - a dict (one child element of type)
        - or a list of dict (many child element of type)

        This behavior can be confusing, and is an issue with the original xml spec being referenced.

        **This method deviates by standardizing to the latter case (always a `list[dict]`).**

        **All XML elements will be a list of dictionaries, even if there's only one element.**

    ```pycon
    >>> xmltodict_parse("")
    Traceback (most recent call last):
    xml.parsers.expat.ExpatError: no element found: line 1, column 0
    >>> xmltodict_parse("<a></a>")
    {'a': None}
    >>> xmltodict_parse("<a foo='bar'></a>")
    {'a': [{'@foo': 'bar'}]}
    >>> xmltodict_parse("<a foo='bar'><foo bar='baz'></foo></a>")  # Singleton - gets modified
    {'a': [{'@foo': 'bar', 'foo': [{'@bar': 'baz'}]}]}
    >>> xmltodict_parse("<a foo='bar'><foo bar='baz'><bar><bop></bop></bar></foo></a>")  # Nested Singletons - modified
    {'a': [{'@foo': 'bar', 'foo': [{'@bar': 'baz', 'bar': [{'bop': None}]}]}]}
    >>> xmltodict_parse("<a foo='bar'><foo bar='baz'></foo><foo bing='bop'></foo></a>")
    {'a': [{'@foo': 'bar', 'foo': [{'@bar': 'baz'}, {'@bing': 'bop'}]}]}

    ```
    :param input_str: The XML string to parse
    :type input_str: str
    :return: The parsed XML
    :rtype: dict
    """
    import xmltodict

    # noinspection t
    def _fix(d):
        """fix the dict in place, recursively, standardizing on a list of dict even if there's only one entry."""
        # if it's a dict, descend to fix
        if isinstance(d, dict):
            for k, v in d.items():
                # @keys are properties of elements, non-@keys are elements
                if not k.startswith("@"):
                    if isinstance(v, dict):
                        # THE FIX
                        # any non-@keys should be a list of dict, even if there's just one of the element
                        d[k] = [v]
                        _fix(v)
                    else:
                        _fix(v)
        # if it's a list, descend to fix
        if isinstance(d, list):
            for v in d:
                _fix(v)

    output = xmltodict.parse(input_str)
    _fix(output)
    return output


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
        raise TypeError(
            "Attempting to add a task without a `task_id` or `task_group_id` attribute"
        )

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


# noinspection t
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
        - **For each**: Apply the [`TranslationRuleset.dag_ruleset`][orbiter.rules.rulesets.DAGRuleset],
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
    if not isinstance(translation_ruleset, TranslationRuleset):
        raise RuntimeError(
            f"Error! type(translation_ruleset)=={type(translation_ruleset)}!=TranslationRuleset! Exiting!"
        )

    # Create an initial OrbiterProject
    project = OrbiterProject()

    for i, (file, input_dict) in enumerate(
        translation_ruleset.get_files_with_extension(input_dir)
    ):
        logger.info(f"Translating [File {i}]={file.resolve()}")

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
        !!! tip

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
            should_show_input = "val" in kwargs and not (
                isinstance(kwargs["val"], OrbiterProject)
                or isinstance(kwargs["val"], OrbiterDAG)
            )
            if result is not None:
                logger.debug(
                    "---------\n"
                    f"[RULESET MATCHED] '{self.__class__.__module__}.{self.__class__.__name__}'\n"
                    f"[RULE MATCHED] '{_rule.__name__}'\n"
                    f"[INPUT] {kwargs if should_show_input else '<Skipping...>'}\n"
                    f"[RETURN] {result}\n"
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


EMPTY_RULESET = {"ruleset": [EMPTY_RULE]}
"""Empty ruleset, for testing"""


class TranslationRuleset(BaseModel, ABC, extra="forbid"):
    """
    A `Ruleset` is a collection of [`Rules`][orbiter.rules.Rule] that are
    evaluated in priority order

    A `TranslationRuleset` is a container for [`Rulesets`][orbiter.rules.rulesets.Ruleset],
    which applies to a specific translation

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
    :param task_filter_ruleset: [`TaskFilterRuleset`][orbiter.rules.rulesets.TaskFilterRuleset]
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

    @validate_call
    def loads(self, input_str: str) -> dict:
        """
        Converts all files of type into a Python dictionary "intermediate representation" form,
        prior to any rulesets being applied.

        | FileType | Conversion Method                                           |
        |----------|-------------------------------------------------------------|
        | `XML`    | [`xmltodict_parse`][orbiter.rules.rulesets.xmltodict_parse] |
        | `YAML`   | `yaml.safe_load`                                            |
        | `JSON`   | `json.loads`                                                |

        :param input_str: The string to convert to a dictionary
        :type input_str: str
        :return: The dictionary representation of the input_str
        :rtype: dict
        """

        if self.file_type == FileType.JSON:
            import json

            return json.loads(input_str)
        elif self.file_type == FileType.YAML:
            import yaml

            return yaml.safe_load(input_str)
        elif self.file_type == FileType.XML:
            return xmltodict_parse(input_str)
        else:
            raise NotImplementedError(f"Cannot load file_type={self.file_type}")

    @validate_call
    def dumps(self, input_dict: dict) -> str:
        """
        Convert Python dictionary back to source string form, useful for testing

        | FileType | Conversion Method   |
        |----------|---------------------|
        | `XML`    | `xmltodict.unparse` |
        | `YAML`   | `yaml.safe_dump`    |
        | `JSON`   | `json.dumps`        |

        :param input_dict: The dictionary to convert to a string
        :type input_dict: dict
        :return str: The string representation of the input_dict, in the file_type format
        :rtype: str
        """
        if self.file_type == FileType.JSON:
            import json

            return json.dumps(input_dict, indent=2)
        elif self.file_type == FileType.YAML:
            import yaml

            return yaml.safe_dump(input_dict)
        elif self.file_type == FileType.XML:
            import xmltodict

            return xmltodict.unparse(input_dict)
        else:
            raise NotImplementedError(f"Cannot dump file_type={self.file_type}")

    def get_files_with_extension(self, input_dir: Path) -> Generator[Path, dict]:
        """
        A generator that yields files with a specific extension(s) in a directory

        :param input_dir: The directory to search in
        :type input_dir: Path
        :return: Generator item of (Path, dict) for each file found
        :rtype: Generator[Path, dict]
        """
        extension = f".{self.file_type.value.lower()}"
        extensions = [extension]

        # YAML and YML are both valid extensions
        extension_sub = {
            "yaml": "yml",
        }
        if other_extension := extension_sub.get(self.file_type.value.lower()):
            extensions.append(f".{other_extension}")

        logger.debug(f"Finding files with extension={extensions} in {input_dir}")
        for directory, _, files in input_dir.walk():
            logger.debug(f"Checking directory={directory}")
            for file in files:
                file = directory / file
                if file.suffix.lower() in extensions:
                    logger.debug(f"File={file} matches extension={extensions}")
                    yield (
                        # Return the file path
                        file,
                        # and load the file and convert it into a python dict
                        self.loads(file.read_text()),
                    )

    def test(self, input_value: str | dict) -> OrbiterProject:
        """
        Test an input against the whole ruleset.
        - 'input_dict' (a parsed python dict)
        - or 'input_str' (raw value) to test against the ruleset.

        :param input_value: The input to test
            can be either a dict (passed to `translate_ruleset.dumps()` before `translate_ruleset.loads()`)
            or a string (read directly by `translate_ruleset.loads()`)
        :type input_value: str | dict
        :return: OrbiterProject produced after applying the ruleset
        :rtype: OrbiterProject
        """
        with TemporaryDirectory() as tempdir:
            file = Path(tempdir) / f"{uuid.uuid4()}.{self.file_type.value}"
            file.write_text(
                self.dumps(input_value)
                if isinstance(input_value, dict)
                else input_value
            )
            return self.translate_fn(translation_ruleset=self, input_dir=file.parent)


if __name__ == "__main__":
    import doctest

    doctest.testmod(
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL
    )
