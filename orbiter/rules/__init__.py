"""
The "brain" of the Orbiter framework is in it's [`Rules`][orbiter.rules.Rule]
and the [`Rulesets`][orbiter.rules.rulesets.Ruleset] that contain them.

- A [`Rule`][orbiter.rules.Rule] contains a python function that is evaluated and produces something
(typically an [Orbiter Object](../objects)) or nothing
- A [`Ruleset`][orbiter.rules.rulesets.Ruleset] is a collection of [`Rules`][orbiter.rules.Rule] that are
    evaluated in priority order
- A [`TranslationRuleset`][orbiter.rules.rulesets.TranslationRuleset]
    is a collection of [`Rulesets`][orbiter.rules.rulesets.Ruleset]
    that relate to a specific [Origin](../origins) and File Type (e.g. `.json`, `.xml`, etc.),
    with a specific `translation_fn`
    (default: [`orbiter.rules.rulesets.translation`][orbiter.rules.rulesets.translate])
    which determines how to apply the [`rulesets`][orbiter.rules.rulesets.Ruleset] against input data.

Different [`Rules`][orbiter.rules.Rule] are applied in different scenarios;
such as for converting input to a DAG ([`@dag_rule`][orbiter.rules.DAGRule]),
or a specific Airflow Operator ([`@task_rule`][orbiter.rules.TaskRule]),
or for filtering entries from the input data
([`@dag_filter_rule`][orbiter.rules.DAGFilterRule], [`@task_filter_rule`][orbiter.rules.TaskFilterRule]).

A [`Rule`][orbiter.rules.Rule] should evaluate to a **single _something_** or nothing.

!!! tip

    If we want to map the following input
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
    ```

    This returns a
    [`OrbiterBashOperator`][orbiter.objects.operators.bash.OrbiterBashOperator], which will become an Airflow
    [BashOperator](https://registry.astronomer.io/providers/apache-airflow/versions/latest/modules/BashOperator)
    when the translation completes.
"""

from __future__ import annotations

import functools
import re
from typing import Callable, Any, Collection, TYPE_CHECKING, List

from pydantic import BaseModel, Field

from orbiter.objects.task import OrbiterOperator, OrbiterTaskDependency

if TYPE_CHECKING:
    from orbiter.objects.task_group import OrbiterTaskGroup
    from orbiter.objects.project import OrbiterProject
    from orbiter.objects.dag import OrbiterDAG

qualname_validator_regex = r"^[\w.]+$"
qualname_validator = re.compile(qualname_validator_regex)


def rule(
    func=None, *, priority=None
) -> (
    Rule
    | "DAGFilterRule"
    | "DAGRule"
    | "TaskFilterRule"
    | "TaskRule"
    | "TaskDependencyRule"
    | "PostProcessingRule"
):
    if func is None:
        return functools.partial(rule, priority=priority)

    priority = priority or 1
    _rule = Rule(priority=priority, rule=func)
    functools.update_wrapper(_rule, func)
    return _rule


class Rule(BaseModel, Callable, extra="forbid"):
    """
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

        If the returned value is an [Orbiter Object](../../objects),
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
        result = self.rule(*args, **kwargs)
        # Save the original kwargs under orbiter_kwargs
        if result:
            if kwargs and hasattr(result, "orbiter_kwargs"):
                setattr(result, "orbiter_kwargs", kwargs)
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
    """Takes a `dict` and returns a `list[dict]` of possible-DAG entries or `None`"""


dag_filter_rule: Callable[[...], DAGFilterRule] = rule


class DAGRule(Rule):
    """A `@dag_rule` decorator creates a [`DAGRule`][orbiter.rules.DAGRule]

    ```python
    @dag_rule
    def foo(val: dict) -> List[dict]:
        return OrbiterDAG(dag_id="foo")
    ```
    """

    rule: Callable[[dict], OrbiterDAG | None]
    """Takes a `dict` and returns an [`OrbiterDAG`][orbiter.objects.dag.OrbiterDAG] or `None`"""


dag_rule: Callable[[...], DAGRule] = rule


class TaskFilterRule(Rule):
    """A `@task_filter_rule` decorator creates a [`TaskFilterRule`][orbiter.rules.TaskFilterRule]

    ```python
    @task_filter_rule
    def foo(val: dict) -> List[dict]:
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
    """Takes a `dict` and returns `list[dict]` or `None`"""


task_filter_rule: Callable[[...], TaskFilterRule] = rule


class TaskRule(Rule):
    """A `@task_rule` decorator creates a [`TaskRule`][orbiter.rules.TaskRule]

    ```python
    @task_rule
    def foo(val: dict) -> OrbiterOperator | OrbiterTaskGroup:
        return OrbiterOperator(task_id="foo")
    ```

    :param val: A dictionary of the task
    :type val: dict
    :return: An subclass of [`OrbiterOperator`][orbiter.objects.task.OrbiterOperator]
        or [`OrbiterTaskGroup`][orbiter.objects.task_group.OrbiterTaskGroup] or `None`
    :rtype: OrbiterOperator | OrbiterTaskGroup | None
    """

    rule: Callable[[dict], OrbiterOperator | OrbiterTaskGroup | None]
    # """Takes a `dict` and returns an subclass of
    # [`OrbiterOperator`][orbiter.objects.task.OrbiterOperator],
    # or [`OrbiterTaskGroup`][orbiter.objects.task_group.OrbiterTaskGroup],
    # or `None`
    # """


task_rule: Callable[[...], TaskRule] = rule


class TaskDependencyRule(Rule):
    """
    An `@task_dependency_rule` decorator creates a [`TaskDependencyRule`][orbiter.rules.TaskDependencyRule],
    which takes an [`OrbiterDAG`][orbiter.objects.dag.OrbiterDAG]
    and returns a [`list[OrbiterTaskDependency]`][orbiter.objects.task.OrbiterTaskDependency] or `None`

    ```python
    @task_dependency_rule
    def foo(val: OrbiterDAG) -> OrbiterTaskDependency:
        return [OrbiterTaskDependency(task_id="task_id", downstream="downstream")]
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
        val.dags["foo"].tasks["bar"].description = "Hello World"
    ```

    :param val: An [`OrbiterProject`][orbiter.objects.project.OrbiterProject]
    :type val: OrbiterProject
    :return: `None`
    :rtype: None
    """
    rule: Callable[[OrbiterProject], None]


post_processing_rule: Callable[[...], PostProcessingRule] = rule


EMPTY_RULE = Rule(rule=lambda _: None, priority=0)
"""Empty rule, for testing"""


if __name__ == "__main__":
    import doctest

    doctest.testmod(
        optionflags=doctest.ELLIPSIS
        | doctest.NORMALIZE_WHITESPACE
        | doctest.IGNORE_EXCEPTION_DETAIL
    )
