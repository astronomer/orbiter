"""
The brain of the Orbiter framework is in it's [`Rules`][orbiter.rules.Rule]
and the [`Rulesets`][orbiter.rules.rulesets.Ruleset] that contain them.

- A [`Rule`][orbiter.rules.Rule] is a python function that is evaluated and produces **something**
(typically an [Object](../objects/index.md)) or **nothing**
- A [`Ruleset`][orbiter.rules.rulesets.Ruleset] is a collection of [`Rules`][orbiter.rules.Rule] that are
    evaluated in priority order
- A [`TranslationRuleset`][orbiter.rules.rulesets.TranslationRuleset]
    is a collection of [`Rulesets`][orbiter.rules.rulesets.Ruleset],
    relating to an [Origin](../origins.md) and [`FileType`][orbiter.file_types.FileType],
    with a [`translate_fn`][orbiter.default_translation.translate] which determines how to apply the rulesets.

Different [`Rules`][orbiter.rules.Rule] are applied in different scenarios, such as:

- converting input to an Airflow DAG ([`@dag_rule`][orbiter.rules.DAGRule]),
- converting input to a specific Airflow Operator ([`@task_rule`][orbiter.rules.TaskRule]),
- filtering entries from the input data
([`@dag_filter_rule`][orbiter.rules.DAGFilterRule], [`@task_filter_rule`][orbiter.rules.TaskFilterRule]).

!!! tip

    To map the following input
    ```json
    {
        "id": "my_task",
        "command": "echo 'hi'"
    }
    ```

    to an Airflow
    [BashOperator](https://registry.astronomer.io/providers/apache-airflow/versions/latest/modules/BashOperator),
    a [`Rule`][orbiter.rules.Rule] could parse it as follows:

    ```python
    @task_rule
    def my_rule(val: dict):
        if 'command' in val:
            return OrbiterBashOperator(task_id=val['id'], bash_command=val['command'])
        else:
            return None
    ```

    This returns a
    [`OrbiterBashOperator`][orbiter.objects.operators.bash.OrbiterBashOperator], which will become an Airflow
    [BashOperator](https://registry.astronomer.io/providers/apache-airflow/versions/latest/modules/BashOperator)
    when the translation completes.
"""

from __future__ import annotations

import functools
import inspect
from textwrap import dedent
from typing import Callable, Any, Collection, TYPE_CHECKING, List

from pydantic import BaseModel, Field

from loguru import logger

from orbiter import trim_dict
from orbiter.meta import OrbiterMeta, VisitTrackedDict
from orbiter.objects.operators.unmapped import OrbiterUnmappedOperator
from orbiter.objects.task import OrbiterOperator, OrbiterTaskDependency

if TYPE_CHECKING:
    from orbiter.objects.task_group import OrbiterTaskGroup
    from orbiter.objects.project import OrbiterProject
    from orbiter.objects.dag import OrbiterDAG


def rule(
    func=None, *, priority=None, params_doc=None
) -> "Rule | DAGFilterRule | DAGRule | TaskFilterRule | TaskRule | TaskDependencyRule | PostProcessingRule":
    if func is None:
        # noinspection PyTypeChecker
        return functools.partial(rule, priority=priority, params_doc=params_doc)

    priority = priority or 1
    _rule = Rule(priority=priority, rule=func, params_doc=params_doc)
    functools.update_wrapper(_rule, func)
    return _rule


class Rule(BaseModel, Callable, extra="forbid"):
    """
    A `Rule` contains a python function that is evaluated and produces something
    (typically an [Object](../objects/index.md)) or nothing

    A `Rule` can be created from a decorator
    ```pycon
    >>> @rule(priority=1)
    ... def my_rule(val):
    ...     return 1
    >>> isinstance(my_rule, Rule)
    True
    >>> my_rule(val={})
    1

    ```

    The function in a rule takes one parameter (`val`), and **must always evaluate to *something* or *nothing*.**
    ```pycon
    >>> some_rule = Rule(rule=lambda val: 4)  # returns something
    >>> some_rule(val={})  # always takes "val", in this case a dict
    4
    >>> other_rule = Rule(rule=lambda val: None)  # returns nothing
    >>> other_rule(val={})

    ```
    Depending on the type of rule, the input `val` may be a dictionary or a different type.
    Refer to each type of rule for more details.

    !!! tip

        If the returned value is an [Orbiter Object](../objects/index.md),
        the input `kwargs` are saved in a special `orbiter_kwargs` property,
        along with information about the matching rule in an `orbiter_meta` property.

        ```pycon
        >>> from orbiter.rules import task_rule
        >>> @task_rule(params_doc={"id": "BashOperator.task_id", "command": "BashOperator.bash_command"})
        ... def my_rule(val: dict):
        ...     '''This rule takes that and gives this'''
        ...     from orbiter.objects.operators.bash import OrbiterBashOperator
        ...     if 'command' in val:
        ...         return OrbiterBashOperator(task_id=val['id'], bash_command=val['command'])
        ...     else:
        ...         return None
        >>> kwargs = {"id": "foo", "command": "echo 'hello world'", "unvisited_key": "bar"}
        >>> match = my_rule(val=kwargs)
        >>> type(match)
        <class 'orbiter.objects.operators.bash.OrbiterBashOperator'>
        >>> match.orbiter_kwargs
        {'val': {'id': 'foo', 'command': "echo 'hello world'", 'unvisited_key': 'bar'}}
        >>> match.orbiter_meta
        ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        OrbiterMeta(matched_rule_source='@task_rule...',
            matched_rule_docstring='This rule takes that and gives this',
            matched_rule_params_doc={'id': 'BashOperator.task_id', 'command': 'BashOperator.bash_command'},
            matched_rule_name='my_rule',
            matched_rule_priority=1,
            visited_keys=['command', 'id'])

        ```

    !!! note

        A `Rule` must have a `rule` property and extra properties cannot be passed

        ```pycon
        >>> # noinspection Pydantic
        ... Rule(rule=lambda: None, not_a_prop="???")
        ... # doctest: +ELLIPSIS
        Traceback (most recent call last):
        pydantic_core._pydantic_core.ValidationError: ...

        ```

    :param rule: Python function to evaluate. Takes a single argument and returns **something** or **nothing**
    :type rule: Callable[[dict | Any], Any | None]
    :param priority: Higher priority rules are evaluated first, must be greater than 0. Default is 0
    :type priority: int, optional
    :param params_doc: What the rule takes as input (key), and gives as output (value)
    :type params_doc: dict[str, str], optional
    """

    rule: Callable[[dict | Any], Any | None]
    priority: int = Field(0, ge=0)
    params_doc: dict[str, str] | None = None

    def __call__(self, *args, **kwargs):
        try:
            tracked_args = [VisitTrackedDict(v) if isinstance(v, dict) else v for v in args]
            tracked_kwargs = {k: VisitTrackedDict(v) if isinstance(v, dict) else v for k, v in kwargs.items()}
            result = self.rule(*tracked_args, **tracked_kwargs)
            if result:
                # Save the original kwargs under orbiter_kwargs
                if kwargs and hasattr(result, "orbiter_kwargs"):
                    setattr(result, "orbiter_kwargs", kwargs)

                # Save meta information about the match
                if hasattr(result, "orbiter_meta"):
                    setattr(
                        result,
                        "orbiter_meta",
                        OrbiterMeta(
                            matched_rule_name=self.rule.__name__,
                            matched_rule_priority=self.priority,
                            matched_rule_params_doc=self.params_doc,
                            matched_rule_docstring=self.rule.__doc__,
                            matched_rule_source=dedent(inspect.getsource(self.rule)),
                            visited_keys=functools.reduce(
                                lambda acc, v: acc + (v.get_visited() if isinstance(v, VisitTrackedDict) else []),
                                tracked_args + list(tracked_kwargs.values()),
                                [],
                            ),
                        ),
                    )
        except Exception as e:
            logger.warning(
                f"[RULE]: {self.rule.__name__}\n[ERROR]:\n{type(e)} - {trim_dict(e)}\n[INPUT]:\n{trim_dict(kwargs)}"
            )
            result = None
        return result


def pattern(func=None, *, params_doc: dict | None = None) -> "Pattern":
    if func is None:
        # noinspection PyTypeChecker
        return functools.partial(pattern, params_doc=params_doc)
    _pattern = Pattern(pattern=func, params_doc=params_doc)
    functools.update_wrapper(_pattern, func)
    return _pattern


class Pattern(BaseModel, Callable, extra="forbid"):
    """Rules can contain many patterns, which represent a subset of specific input matched to a specific output.

    Patterns are just functions that can have a `params_doc` property,
    which is used to document the input and output of the pattern.

    Patterns provide overall composability and reusability.

    ```pycon
    >>> from orbiter.objects.operators.bash import OrbiterBashOperator
    >>> @pattern(params_doc={"command": "Operator.bash_command"})
    ... def command_pattern(val: dict) -> dict:
    ...   if 'command' in val:
    ...     return {"bash_command": val['command']}
    ...   else:
    ...     return {}
    ...
    >>> @rule(params_doc={"id": "Operator.task_id"} | command_pattern.params_doc)
    ... def bash_rule(val: dict) -> OrbiterBashOperator | None:
    ...     return OrbiterBashOperator(
    ...         task_id=val['id'],
    ...         **command_pattern(val),
    ...     ) if 'id' in val and 'command' in val else None
    >>> bash_rule(val={"id": "foo", "command": "echo 'hello world'"})
    foo_task = BashOperator(task_id='foo', bash_command="echo 'hello world'")

    ```
    """

    pattern: Callable[..., dict | Any | None]
    params_doc: dict[str, str] | None = None

    def __call__(self, *args, **kwargs):
        return self.pattern(*args, **kwargs)


class DAGFilterRule(Rule):
    """The `@dag_filter_rule` decorator creates a [`DAGFilterRule`][orbiter.rules.DAGFilterRule]

    ```python
    @dag_filter_rule
    def foo(val: dict) -> List[dict]:
        return [{"dag_id": "foo"}]
    ```

    !!! hint

        In addition to filtering, a `DAGFilterRule` can also map input to a more reasonable output for later processing
    """

    rule: Callable[[dict], Collection[dict] | None]


dag_filter_rule: Callable[[...], DAGFilterRule] = rule


class DAGRule(Rule):
    """A `@dag_rule` decorator creates a [`DAGRule`][orbiter.rules.DAGRule]

    !!! tip

        A `__file` key is added to the original input, which is the file path of the input.

    ```python
    @dag_rule
    def foo(val: dict) -> OrbiterDAG | None:
        if "id" in val:
            return OrbiterDAG(dag_id=val["id"], file_path=f"{val['id']}.py")
        else:
            return None
    ```
    """

    rule: Callable[[dict], OrbiterDAG | None]


dag_rule: Callable[[...], DAGRule] = rule


class TaskFilterRule(Rule):
    # noinspection PyUnresolvedReferences
    """A `@task_filter_rule` decorator creates a [`TaskFilterRule`][orbiter.rules.TaskFilterRule]

    ```python
    @task_filter_rule
    def foo(val: dict) -> List[dict] | None:
        return [{"task_id": "foo"}]
    ```

    !!! hint

        In addition to filtering, a `TaskFilterRule` can also map input to a more reasonable output for later processing

    :param val: A dictionary of the task
    :type val: dict
    :return: A list of dictionaries of possible tasks or `None`
    :rtype: List[dict] | None
    """

    rule: Callable[[dict], Collection[dict] | None]


task_filter_rule: Callable[[...], TaskFilterRule] = rule


class TaskRule(Rule):
    # noinspection PyUnresolvedReferences
    """A `@task_rule` decorator creates a [`TaskRule`][orbiter.rules.TaskRule]

    ```python
    @task_rule
    def foo(val: dict) -> OrbiterOperator | OrbiterTaskGroup:
        if "id" in val and "command" in val:
            return OrbiterBashOperator(
                task_id=val["id"], bash_command=val["command"]
            )
        else:
            return None
    ```

    :param val: A dictionary of the task
    :type val: dict
    :return: A subclass of [`OrbiterOperator`][orbiter.objects.task.OrbiterOperator]
        or [`OrbiterTaskGroup`][orbiter.objects.task_group.OrbiterTaskGroup] or `None`
    :rtype: OrbiterOperator | OrbiterTaskGroup | None
    """

    rule: Callable[[dict], OrbiterOperator | OrbiterTaskGroup | None]


task_rule: Callable[[...], TaskRule] = rule


class TaskDependencyRule(Rule):
    # noinspection PyUnresolvedReferences
    """
    An `@task_dependency_rule` decorator creates a [`TaskDependencyRule`][orbiter.rules.TaskDependencyRule],
    which takes an [`OrbiterDAG`][orbiter.objects.dag.OrbiterDAG]
    and returns a [`list[OrbiterTaskDependency]`][orbiter.objects.task.OrbiterTaskDependency] or `None`

    ```python
    @task_dependency_rule
    def foo(val: OrbiterDAG) -> OrbiterTaskDependency:
        return [OrbiterTaskDependency(task_id="upstream", downstream="downstream")]
    ```

    :param val: An [`OrbiterDAG`][orbiter.objects.dag.OrbiterDAG]
    :type val: OrbiterDAG
    :return: A list of [`OrbiterTaskDependency`][orbiter.objects.task.OrbiterTaskDependency] or `None`
    :rtype: List[OrbiterTaskDependency] | None
    """

    rule: Callable[[OrbiterDAG], List[OrbiterTaskDependency] | None]


task_dependency_rule: Callable[[...], TaskDependencyRule] = rule


class PostProcessingRule(Rule):
    # noinspection PyUnresolvedReferences
    """
    An `@post_processing_rule` decorator creates a [`PostProcessingRule`][orbiter.rules.PostProcessingRule],
    which takes an [`OrbiterProject`][orbiter.objects.project.OrbiterProject],
    after all other rules have been applied, and modifies it in-place.

    ```python
    @post_processing_rule
    def foo(val: OrbiterProject) -> None:
        val.dags["foo"].tasks["bar"].doc = "Hello World"
    ```

    :param val: An [`OrbiterProject`][orbiter.objects.project.OrbiterProject]
    :type val: OrbiterProject
    :return: `None`
    :rtype: None
    """

    rule: Callable[[OrbiterProject], None]


post_processing_rule: Callable[[...], PostProcessingRule] = rule


def create_cannot_map_rule_with_task_id_fn(task_id_fn: Callable[[dict], str]) -> TaskRule | Callable:
    """Inject a `task_id` generator function into the "Cannot Map" Rule."""

    @task_rule(priority=1)
    def cannot_map_rule(val: dict) -> OrbiterUnmappedOperator | None:
        """Can be used in a TaskRuleset.
        Returns an `OrbiterUnmappedOperator` with a doc string that says it cannot map the task.
        Useful to ensure that tasks that cannot be mapped are still visible in the output.
        """
        from orbiter.objects.operators.unmapped import OrbiterUnmappedOperator

        return OrbiterUnmappedOperator(
            task_id=task_id_fn(val),
            doc_md="""[task_type=UNKNOWN] Input did not translate""",
            source=str(trim_dict(val)),
        )

    return cannot_map_rule


cannot_map_rule = create_cannot_map_rule_with_task_id_fn(lambda val: "UNKNOWN")

EMPTY_RULE = Rule(rule=lambda val: None, priority=0)
"""Empty rule, for testing"""
