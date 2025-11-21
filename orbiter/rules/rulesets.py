from __future__ import annotations

import functools
import inspect
import uuid
from abc import ABC, abstractmethod
from itertools import chain
from operator import add
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import (
    Annotated,
    Any,
    Callable,
    Collection,
    Generator,
    List,
    Set,
    Type,
    Mapping,
    Union,
    Tuple,
)

from loguru import logger
from pydantic import AfterValidator, BaseModel, validate_call, Field
from pydantic_settings import BaseSettings

from orbiter import import_from_qualname, trim_dict, QualifiedImport, validate_qualified_imports, _backport_walk
from orbiter.default_translation import translate, fake_translate
from orbiter.file_types import FileType, FileTypeJSON
from orbiter.objects.dag import OrbiterDAG
from orbiter.objects.project import OrbiterProject
from orbiter.objects.task import OrbiterOperator, OrbiterTaskDependency
from orbiter.objects.task_group import OrbiterTaskGroup
from orbiter.rules import (
    EMPTY_RULE,
    DAGFilterRule,
    DAGRule,
    PostProcessingRule,
    Rule,
    TaskDependencyRule,
    TaskFilterRule,
    TaskRule,
)


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


TranslateFn = Annotated[
    Union[QualifiedImport, Callable[["TranslationRuleset", Path], OrbiterProject]],
    AfterValidator(validate_translate_fn),
]


class Ruleset(BaseModel, ABC, frozen=True, extra="forbid"):
    """A list of rules, which are evaluated to generate different types of output

    You must pass a [`Rule`][orbiter.rules.Rule] (or `dict` with the schema of [`Rule`][orbiter.rules.Rule])
    ```pycon
    >>> from orbiter.rules import rule
    >>> @rule
    ... def x(val):
    ...    return None
    >>> GenericRuleset(ruleset=[x, {"rule": lambda: None}])
    ... # doctest: +ELLIPSIS
    GenericRuleset(ruleset=[Rule(...), ...])

    ```

    !!! note

        You can't pass non-Rules
        ```pycon
        >>> # noinspection PyTypeChecker
        ... GenericRuleset(ruleset=[None])
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
        >>> ruleset = GenericRuleset(ruleset=[filter_for_type_folder])
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
            if (results := self.apply(take_first=False, val=item)) is not None and len(results)
        ]

    def _sorted(self) -> List[Rule]:
        """Return a copy of the ruleset, sorted by priority
        ```pycon
        >>> sorted_rules = GenericRuleset(ruleset=[
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
        >>> GenericRuleset(ruleset=[gt_4]).apply(val=5)
        ['5']

        ```

        Many rules can produce many results, one for each rule.
        ```pycon
        >>> @rule
        ... def gt_3(val):
        ...    return str(val) if val > 3 else None
        >>> GenericRuleset(ruleset=[gt_4, gt_3]).apply(val=5)
        ['5', '5']

        ```

        The `take_first` flag will evaluate rules in the ruleset and return the first match
        ```pycon
        >>> GenericRuleset(ruleset=[gt_4, gt_3]).apply(val=5, take_first=True)
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
        >>> GenericRuleset(ruleset=[always_none, more_always_none]).apply(val=5)
        []

        ```

        If nothing matched, and `take_first=True`, `None` is returned
        ```pycon
        >>> GenericRuleset(ruleset=[always_none, more_always_none]).apply(val=5, take_first=True)
        ... # None

        ```

        !!! tip

            If no input is given, an error is returned
            ```pycon
            >>> GenericRuleset(ruleset=[always_none]).apply()
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
            raise RuntimeError("No values provided! Supply at least one key=val pair as kwargs!")
        results = []
        for _rule in self._sorted():
            result = _rule(**kwargs)
            should_show_input = "val" in kwargs and not (
                isinstance(kwargs["val"], OrbiterProject) or isinstance(kwargs["val"], OrbiterDAG)
            )
            if result is not None:
                logger.debug(
                    "---------\n"
                    f"[RULESET MATCHED] '{self.__class__.__module__}.{self.__class__.__name__}'\n"
                    f"[RULE MATCHED] '{_rule.__name__}'\n"
                    f"[INPUT] {trim_dict(kwargs) if should_show_input else '<Skipping...>'}\n"
                    f"[RETURN] {trim_dict(result)}\n"
                    f"---------"
                )
                results.append(result)
                if take_first:
                    return result
        return None if take_first and not len(results) else results

    @abstractmethod
    def apply_ruleset(self, **kwargs) -> Any:
        pass


class DAGFilterRuleset(Ruleset):
    """Ruleset of [`DAGFilterRule`][orbiter.rules.DAGFilterRule]"""

    ruleset: List[DAGFilterRule | Rule | Callable[[dict], Collection[dict] | None] | dict]

    def apply_ruleset(self, input_dict: dict, file: Path, input_dir: Path) -> list[dict]:
        """Apply all rules from [DAG Filter Ruleset][orbiter.rules.rulesets.DAGFilterRuleset] to filter down to keys
        that look like they can be translated to a DAG, in priority order.

        !!! note

            The file is added under a `__file` key to both input and output dictionaries, for use in future rules.
            The input dir is added under a `__input_dir` key to both input and output dictionaries, for use in future rules.

        :param input_dict: The input dictionary to filter, e.g. the file that was loaded and converted into a python dict
        :param file: The file relative to the input directory's parent (is added under '__file' key)
        :param input_dir: The input directory (is added under '__input_dir' key)
        :return: A list of dictionaries that look like they can be translated to a DAG
        """

        def with_file(d: dict) -> dict:
            try:
                return {"__file": file, "__input_dir": input_dir} | d
            except TypeError as e:
                logger.opt(exception=e).debug("Unable to add one or both of `__file` or `__input_dir` keys")
                return d

        return [with_file(dag_dict) for dag_dict in functools.reduce(add, self.apply(val=with_file(input_dict)), [])]


class DAGRuleset(Ruleset):
    """Ruleset of [`DAGRule`][orbiter.rules.DAGRule]"""

    ruleset: List[DAGRule | Rule | Callable[[dict], OrbiterDAG | None] | dict]

    def apply_ruleset(self, dag_dict: dict) -> OrbiterDAG | None:
        """Apply all rules from `DAGRuleset` to convert the object to an `OrbiterDAG`,
        in priority order, stopping when the first rule returns a match.
        If no rule returns a match, the entry is filtered.

        !!! note

            The file is retained via `OrbiterDAG.orbiter_kwargs['val']['__file']`, for use in future rules.

        :param dag_dict: DAG Candidate, filtered via @dag_filter_rules
        :return: An `OrbiterDAG` object
        """
        return self.apply(val=dag_dict, take_first=True)


class TaskFilterRuleset(Ruleset):
    """Ruleset of [`TaskFilterRule`][orbiter.rules.TaskFilterRule]"""

    ruleset: List[TaskFilterRule | Rule | Callable[[dict], Collection[dict] | None] | dict]

    def apply_ruleset(self, dag_dict: dict) -> list[dict]:
        """Apply all rules from `TaskFilterRuleset` to filter down to keys that look like they can be translated
         to a Task, in priority order.

        Many entries in the dag_dict result in many task_dicts.

        :param dag_dict: The dict to filter - the same dict that was passed to the DAG ruleset
        :return: A list of dictionaries that look like they can be translated to a Task
        """
        return functools.reduce(add, self.apply(val=dag_dict), [])


class TaskRuleset(Ruleset):
    """Ruleset of [`TaskRule`][orbiter.rules.TaskRule]"""

    ruleset: List[TaskRule | Rule | Callable[[dict], OrbiterOperator | OrbiterTaskGroup | None] | dict]

    def apply_ruleset(self, task_dict: dict) -> OrbiterOperator | OrbiterTaskGroup:
        """Apply all rules from `TaskRuleset` to convert the object to a Task, in priority order,
        stopping when the first rule returns a match.

        One task_dict makes up to one task (unless it produces a TaskGroup, which can contain many tasks).
        If no rule returns a match, the entry is filtered.

        :param task_dict: A dict to translate - what was returned from the Task Filter ruleset
        :return: An [OrbiterOperator][orbiter.objects.task.OrbiterOperator] (or descendant) or [OrbiterTaskGroup][orbiter.objects.task_group.OrbiterTaskGroup]
        """
        return self.apply(val=task_dict, take_first=True)


class TaskDependencyRuleset(Ruleset):
    """Ruleset of [`TaskDependencyRule`][orbiter.rules.TaskDependencyRule]"""

    ruleset: List[TaskDependencyRule | Rule | Callable[[OrbiterDAG], List[OrbiterTaskDependency] | None] | dict]

    def apply_ruleset(self, dag: OrbiterDAG) -> list[OrbiterTaskDependency]:
        """Apply all rules from `TaskDependencyRuleset` to create a list of
        [`OrbiterTaskDependency`][orbiter.objects.task.OrbiterTaskDependency],
        which are then added in-place to each task in the [`OrbiterDAG`][orbiter.objects.dag.OrbiterDAG].

        :param dag: The DAG to add the task dependencies to
        :return: A list of [`OrbiterTaskDependency`][orbiter.objects.task.OrbiterTaskDependency]
        """
        return list(chain(*self.apply(val=dag))) or []


class PostProcessingRuleset(Ruleset):
    """Ruleset of [`PostProcessingRule`][orbiter.rules.PostProcessingRule]"""

    ruleset: List[PostProcessingRule | Rule | Callable[[OrbiterProject], None] | dict]

    def apply_ruleset(self, project: OrbiterProject) -> None:
        """Apply all rules from `PostProcessingRuleset` to modify project in-place

        :param project: The OrbiterProject to modify
        :return: None
        """
        self.apply(val=project, take_first=False)


class GenericRuleset(Ruleset):
    """Generic ruleset, for testing"""

    def apply_ruleset(self, **kwargs) -> Any:
        pass


EMPTY_RULESET = {"ruleset": [EMPTY_RULE]}
"""Empty ruleset, for testing"""


class TranslationConfig(BaseSettings):
    """
    :param parallel: Whether to run filter steps with parallel processing, default False
    :type parallel: bool
    :param upfront: Whether to run filter steps upfront, prior to all other steps, default False
    :type upfront: bool
    """

    parallel: Annotated[
        bool,
        Field(
            default=False,
            description="Whether to run filter rulesets in parallel, via multi-processing. "
            "This may cause unexpected results if rulesets share global state. "
            "A threadpool is used if running via binary.",
        ),
    ]
    upfront: Annotated[
        bool, Field(default=False, description="Whether to run filter rulesets upfront, prior to all other rulesets")
    ]


class TranslationRuleset(BaseModel, ABC, extra="forbid"):
    """
    A `Ruleset` is a collection of [`Rules`][orbiter.rules.Rule] that are
    evaluated in priority order

    A `TranslationRuleset` is a container for [`Rulesets`][orbiter.rules.rulesets.Ruleset],
    which applies to a specific translation

    ```pycon
    >>> TranslationRuleset(
    ...   file_type={FileTypeJSON},                                      # Has a file type
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

    :param file_type: FileType to translate
    :type file_type: Set[Type[FileType]]
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

    file_type: Set[Type[FileType]]
    config: TranslationConfig = TranslationConfig()
    dag_filter_ruleset: DAGFilterRuleset | dict
    dag_ruleset: DAGRuleset | dict
    task_filter_ruleset: TaskFilterRuleset | dict
    task_ruleset: TaskRuleset | dict
    task_dependency_ruleset: TaskDependencyRuleset | dict
    post_processing_ruleset: PostProcessingRuleset | dict
    translate_fn: TranslateFn = translate

    def get_ext(self) -> str:
        """
        Get the first file extension for this ruleset

        ```pycon
        >>> EMPTY_TRANSLATION_RULESET.get_ext()
        'JSON'

        ```
        """
        return next(iter(next(iter(self.file_type)).extension))

    @validate_call
    def loads(self, file: Path) -> dict:
        """
        Converts all files of type into a Python dictionary "intermediate representation" form,
        prior to any rulesets being applied.

        :param file: The file to load
        :type file: Path
        :return: The dictionary representation of the input_str
        :rtype: dict
        """
        for file_type in self.file_type:
            if file.suffix.lower() in {f".{ext.lower()}" for ext in file_type.extension}:
                try:
                    return file_type.load_fn(file.read_text())
                except Exception as e:
                    logger.error(f"Error loading file={file}! Skipping!\n{e}")
                    continue
        raise TypeError(f"Invalid file_type={file.suffix}, does not match file_type={self.file_type}")

    @validate_call
    def dumps(self, input_dict: dict, ext: str | None = None) -> str:
        """
        Convert Python dictionary back to source string form, useful for testing

        :param input_dict: The dictionary to convert to a string
        :type input_dict: dict
        :param ext: The file type extension to dump as, defaults to first 'file_type' in the set
        :type ext: str | None
        :return str: The string representation of the input_dict, in the file_type format
        :rtype: str
        """
        for file_type in self.file_type:
            if ext is None or ext.lower() in file_type.extension:
                return file_type.dump_fn(input_dict)
        raise TypeError(f"Invalid file_type={ext}")

    def get_files_with_extension(self, input_dir: Path) -> Generator[Tuple[Path, dict]]:
        """
        A generator that yields files with a specific extension(s) in a directory

        :param input_dir: The directory to search in
        :type input_dir: Path
        :return: Generator item of (Path, dict) for each file found
        :rtype: Generator[Path, dict]
        """
        for directory, _, files in input_dir.walk() if hasattr(input_dir, "walk") else _backport_walk(input_dir):
            logger.debug(f"Checking directory={directory}")
            for file in files:
                file = directory / file
                try:
                    dict_or_list = self.loads(file)
                    # If the file is list-like (not a dict), return each item in the list
                    if isinstance(dict_or_list, Collection) and not isinstance(dict_or_list, Mapping):
                        for item in dict_or_list:
                            yield file, item
                    else:
                        yield file, dict_or_list
                except TypeError:
                    logger.debug(f"File={file} not of correct type, skipping...")
                    continue

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
            file = Path(tempdir) / f"{uuid.uuid4()}.{self.get_ext()}"
            file.write_text(self.dumps(input_dict=input_value) if isinstance(input_value, dict) else input_value)
            return self.translate_fn(translation_ruleset=self, input_dir=file.parent)


EMPTY_TRANSLATION_RULESET = TranslationRuleset(
    file_type={FileTypeJSON},
    dag_filter_ruleset=EMPTY_RULESET,
    dag_ruleset=EMPTY_RULESET,
    task_filter_ruleset=EMPTY_RULESET,
    task_ruleset=EMPTY_RULESET,
    task_dependency_ruleset=EMPTY_RULESET,
    post_processing_ruleset=EMPTY_RULESET,
    translate_fn=fake_translate,
)
