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
    with a [`translate_fn`][orbiter.rules.rulesets.translate] which determines how to apply the rulesets.

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
    def my_rule(val):
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
import json
import re
from typing import Callable, Any, Collection, TYPE_CHECKING, List, Mapping

from pydantic import BaseModel, Field

from loguru import logger

from orbiter.config import TRIM_LOG_OBJECT_LENGTH
from orbiter.objects.task import OrbiterOperator, OrbiterTaskDependency

if TYPE_CHECKING:
    from orbiter.objects.task_group import OrbiterTaskGroup
    from orbiter.objects.project import OrbiterProject
    from orbiter.objects.dag import OrbiterDAG

qualname_validator_regex = r"^[\w.]+$"
qualname_validator = re.compile(qualname_validator_regex)


def trim_dict(v):
    """Stringify and trim a dictionary if it's greater than a certain length
    (used to trim down overwhelming log output)"""
    if TRIM_LOG_OBJECT_LENGTH != -1 and isinstance(v, Mapping):
        if len(str(v)) > TRIM_LOG_OBJECT_LENGTH:
            return json.dumps(v, default=str)[:TRIM_LOG_OBJECT_LENGTH] + "..."
    if isinstance(v, list):
        return [trim_dict(_v) for _v in v]
    return v


def rule(
    func=None, *, priority=None
) -> Rule | "DAGFilterRule" | "DAGRule" | "TaskFilterRule" | "TaskRule" | "TaskDependencyRule" | "PostProcessingRule":
    if func is None:
        return functools.partial(rule, priority=priority)

    priority = priority or 1
    _rule = Rule(priority=priority, rule=func)
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
    >>> Rule(rule=lambda val: 4)({})
    4
    >>> Rule(rule=lambda val: None)({})

    ```

    !!! tip

        If the returned value is an [Orbiter Object](../../objects/index.md),
        the passed `kwargs` are saved in a special `orbiter_kwargs` property

        ```pycon
        >>> from orbiter.objects.dag import OrbiterDAG
        >>> @rule
        ... def my_rule(foo):
        ...     return OrbiterDAG(dag_id="", file_path="")
        >>> my_rule(foo="bar").orbiter_kwargs
        {'foo': 'bar'}

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
    """

    rule: Callable[[dict | Any], Any | None]
    priority: int = Field(0, ge=0)

    def __call__(self, *args, **kwargs):
        try:
            result = self.rule(*args, **kwargs)
            # Save the original kwargs under orbiter_kwargs
            if result:
                if kwargs and hasattr(result, "orbiter_kwargs"):
                    setattr(result, "orbiter_kwargs", kwargs)
        except Exception as e:
            logger.warning(
                f"[RULE]: {self.rule.__name__}\n"
                f"[ERROR]:\n{type(e)} - {trim_dict(e)}\n"
                f"[INPUT]:\n{trim_dict(args)}\n{trim_dict(kwargs)}"
            )
            result = None
        return result


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
            return OrbiterDAG(dag_id=val["id"], file_path=f"{val["id"]}.py")
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


@task_rule(priority=1)
def cannot_map_rule(val: dict) -> OrbiterOperator | None:
    """Can be used in a TaskRuleset.
    Returns an `OrbiterEmptyOperator` with a doc string that says it cannot map the task.
    Useful to ensure that tasks that cannot be mapped are still visible in the output.
    """
    from orbiter.objects.operators.empty import OrbiterEmptyOperator

    # noinspection PyArgumentList
    return OrbiterEmptyOperator(
        task_id="UNKNOWN",
        doc_md=f"""[task_type=UNKNOWN] Input did not translate: `{trim_dict(val)}`""",
    )


EMPTY_RULE = Rule(rule=lambda val: None, priority=0)
"""Empty rule, for testing"""


if __name__ == "__main__":
    import doctest

    doctest.testmod(optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.IGNORE_EXCEPTION_DETAIL)
