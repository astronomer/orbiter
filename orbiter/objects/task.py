from __future__ import annotations

import ast
from abc import ABC
from typing import Set, List, Annotated, Callable, Literal, TYPE_CHECKING

from loguru import logger
from pydantic import AfterValidator, BaseModel, validate_call

from orbiter import clean_value
from orbiter.ast_helper import (
    OrbiterASTBase,
    py_function,
    py_bitshift,
)
from orbiter.ast_helper import py_assigned_object
from orbiter.config import ORBITER_TASK_SUFFIX
from orbiter.objects import ImportList, RenderAttributes, CALLBACK_KEYS
from orbiter.objects import OrbiterBase
from orbiter.objects.callbacks.callback_type import CallbackType
from orbiter.objects.pool import OrbiterPool

if TYPE_CHECKING:
    from orbiter.objects.task_group import OrbiterTaskGroup

__mermaid__ = """
--8<-- [start:mermaid-dag-relationships]
OrbiterOperator --> "many" OrbiterRequirement
OrbiterOperator --> "one" OrbiterPool
OrbiterOperator --> "many" OrbiterConnection
OrbiterOperator --> "many" OrbiterVariable
OrbiterOperator --> "many" OrbiterEnvVar
--8<-- [end:mermaid-dag-relationships]

--8<-- [start:mermaid-task-relationships]
OrbiterOperator "implements" <|-- OrbiterTask
OrbiterOperator --> "many" OrbiterCallback
--8<-- [end:mermaid-task-relationships]
"""

TaskId = Annotated[str, AfterValidator(lambda t: to_task_id(t))]


def task_add_downstream(self, task_id: str | List[str] | OrbiterTaskDependency) -> "OrbiterOperator | OrbiterTaskGroup":
    # noinspection PyProtectedMember
    """
    Add a downstream task dependency

    ```pycon
    >>> from orbiter.objects.operators.empty import OrbiterEmptyOperator
    >>> from orbiter.objects.task import task_add_downstream, OrbiterTaskDependency
    >>> from orbiter.ast_helper import render_ast
    >>> render_ast(task_add_downstream(OrbiterEmptyOperator(task_id="task_id"), "downstream")._downstream_to_ast())
    'task_id_task >> downstream_task'
    >>> render_ast(
    ...     task_add_downstream(OrbiterEmptyOperator(task_id="task_id"),
    ...     OrbiterTaskDependency(task_id="task_id", downstream="downstream"))._downstream_to_ast()
    ... )
    'task_id_task >> downstream_task'
    >>> render_ast(task_add_downstream(
    ...     OrbiterEmptyOperator(task_id="task_id"),
    ...     ["downstream"]
    ... )._downstream_to_ast())
    'task_id_task >> downstream_task'
    >>> render_ast(task_add_downstream(
    ...     OrbiterEmptyOperator(task_id="task_id"),
    ...     ["downstream", "downstream2"]
    ... )._downstream_to_ast())
    'task_id_task >> [downstream2_task, downstream_task]'

    ```
    """  # noqa: E501
    if isinstance(task_id, OrbiterTaskDependency):
        task_dependency = task_id
        if task_dependency.task_id != self.task_id:
            raise ValueError(f"task_dependency={task_dependency} has a different task_id than {self.task_id}")
        # do normal parsing logic, but with these downstream items
        task_id = task_dependency.downstream

    if isinstance(task_id, str):
        self.downstream |= {to_task_id(task_id)}
        return self
    else:
        if not len(task_id):
            return self

        downstream_task_id = {to_task_id(t) for t in task_id}
        logger.debug(f"Adding downstream {downstream_task_id} to {self.task_id}")
        self.downstream |= downstream_task_id
        return self


class OrbiterTaskDependency(BaseModel, extra="forbid"):
    """Represents a task dependency, which is added to either an
    [`OrbiterOperator`][orbiter.objects.task.OrbiterOperator]
    or an [`OrbiterTaskGroup`][orbiter.objects.task_group.OrbiterTaskGroup].

    :param task_id: The task_id for the operator
    :type task_id: str
    :param downstream: downstream task(s)
    :type downstream: str | List[str]
    """

    # --8<-- [start:mermaid-td-props]
    task_id: TaskId
    downstream: TaskId | List[TaskId]
    # --8<-- [end:mermaid-td-props]

    def __str__(self):
        return f"{self.task_id} >> {self.downstream}"

    def __repr__(self):
        return str(self)


class OrbiterOperator(OrbiterASTBase, OrbiterBase, ABC, extra="allow"):
    """
    **Abstract class** representing a
    [Task in Airflow](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html#operators).

    **Must be subclassed** (such as [`OrbiterBashOperator`][orbiter.objects.operators.bash.OrbiterBashOperator],
    or [`OrbiterTask`][orbiter.objects.task.OrbiterTask]).

    Subclassing Example:
    ```pycon
    >>> from orbiter.objects import OrbiterRequirement
    >>> class OrbiterMyOperator(OrbiterOperator):
    ...   imports: ImportList = [OrbiterRequirement(package="apache-airflow")]
    ...   operator: str = "MyOperator"

    >>> foo = OrbiterMyOperator(task_id="task_id"); foo
    task_id_task = MyOperator(task_id='task_id')

    ```

    Adding single downstream tasks:
    ```pycon
    >>> from orbiter.ast_helper import render_ast
    >>> render_ast(foo.add_downstream("downstream")._downstream_to_ast())
    'task_id_task >> downstream_task'

    ```

    Adding multiple downstream tasks:
    ```pycon
    >>> render_ast(foo.add_downstream(["a", "b"])._downstream_to_ast())
    'task_id_task >> [a_task, b_task, downstream_task]'

    ```

    !!! note

        Validation - task_id in OrbiterTaskDependency must match this task_id
        ```pycon
        >>> foo.add_downstream(OrbiterTaskDependency(task_id="other", downstream="bar")).downstream
        ... # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ValueError: Task dependency ... has a different task_id than task_id

        ```

    :param imports: List of requirements for the operator
    :type imports: List[OrbiterRequirement]
    :param task_id: The `task_id` for the operator, must be unique and snake_case
    :type task_id: str
    :param trigger_rule: Conditions under which to start the task
        [(docs)](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#trigger-rules)
    :type trigger_rule: str, optional
    :param pool: Name of the
        [pool](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html#pools)
        to use
    :type pool: str, optional
    :param pool_slots: Slots for this task to occupy
    :type pool_slots: int, optional
    :param operator: Operator name
    :type operator: str, optional
    :param downstream: Downstream tasks, defaults to `set()`
    :type downstream: Set[str], optional
    :param **kwargs: Other properties that may be passed to operator
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """

    orbiter_type: Literal["OrbiterOperator"] = "OrbiterOperator"

    imports: ImportList

    operator: str
    task_id: TaskId

    trigger_rule: str | None = None

    pool: str | None = None
    pool_slots: int | None = None
    orbiter_pool: OrbiterPool | None = None
    downstream: Set[str] = set()

    on_success_callback: CallbackType
    on_failure_callback: CallbackType
    on_retry_callback: CallbackType
    on_execute_callback: CallbackType
    on_skipped_callback: CallbackType
    sla_miss_callback: CallbackType

    render_attributes: RenderAttributes = [
        "task_id",
        "pool",
        "pool_slots",
        "trigger_rule",
    ] + CALLBACK_KEYS

    __mermaid__ = """
    --8<-- [start:mermaid-op-props]
    imports: List[OrbiterRequirement]
    operator: str
    task_id: str
    pool: str | None
    pool_slots: int | None
    trigger_rule: str | None
    on_failure_callback: OrbiterCallback
    on_success_callback: OrbiterCallback
    on_retry_callback: OrbiterCallback
    on_skipped_callback: OrbiterCallback
    on_execute_callback: OrbiterCallback
    sla_miss_callback: OrbiterCallback
    downstream: Set[str]
    add_downstream(str | List[str] | OrbiterTaskDependency)
    --8<-- [end:mermaid-op-props]
    """

    def add_downstream(self, task_id: str | List[str] | OrbiterTaskDependency) -> "OrbiterOperator":
        return task_add_downstream(self, task_id)

    def _downstream_to_ast(self) -> List[ast.stmt]:
        if not self.downstream:
            return []
        elif len(self.downstream) == 1:
            (t,) = tuple(self.downstream)
            return py_bitshift(
                to_task_id(self.task_id, ORBITER_TASK_SUFFIX),
                to_task_id(t, ORBITER_TASK_SUFFIX),
            )
        else:
            return py_bitshift(
                to_task_id(self.task_id, ORBITER_TASK_SUFFIX),
                sorted([to_task_id(t, ORBITER_TASK_SUFFIX) for t in self.downstream]),
            )

    def _to_ast(self) -> ast.stmt:
        def prop(k):
            attr = getattr(self, k, None) or getattr(self.model_extra, k, None)
            if isinstance(attr, Callable):
                return ast.Name(id=attr.__name__)
            elif "_callable" in k and isinstance(attr, str):
                return ast.Name(id=attr)
            return attr

        # foo = Bar(x=x,y=y, z=z)
        return py_assigned_object(
            to_task_id(self.task_id, ORBITER_TASK_SUFFIX),
            self.operator,
            **{k: prop(k) for k in self.render_attributes if k and getattr(self, k)},
            **{k: prop(k) for k in (self.model_extra.keys() or [])},
        )


class OrbiterTask(OrbiterOperator, extra="allow"):
    """
    A generic version of [`OrbiterOperator`][orbiter.objects.task.OrbiterOperator] that can be instantiated directly.

    The operator that is instantiated is inferred from the `imports` field,
    via the first `*Operator` or `*Sensor` import.

    [View info for specific operators at the Astronomer Registry.](https://registry.astronomer.io/)

    ```pycon
    >>> from orbiter.objects.requirement import OrbiterRequirement
    >>> OrbiterTask(task_id="foo", bash_command="echo 'hello world'", other=1, imports=[
    ...   OrbiterRequirement(package="apache-airflow", module="airflow.operators.bash", names=["BashOperator"])
    ... ])
    foo_task = BashOperator(task_id='foo', bash_command="echo 'hello world'", other=1)

    >>> def foo():
    ...   pass
    >>> OrbiterTask(task_id="foo", python_callable=foo, other=1, imports=[
    ...   OrbiterRequirement(package="apache-airflow", module="airflow.sensors.python", names=["PythonSensor"])
    ... ])
    def foo():
        pass
    foo_task = PythonSensor(task_id='foo', other=1, python_callable=foo)

    ```

    :param task_id: The `task_id` for the operator. Must be unique and snake_case
    :type task_id: str
    :param imports: List of requirements for the operator.
        The Operator is inferred from first `*Operator` or `*Sensor` imported.
    :type imports: List[OrbiterRequirement]
    :param **kwargs: Any other keyword arguments to be passed to the operator
    """

    orbiter_type: Literal["OrbiterTask"] = "OrbiterTask"

    imports: ImportList
    task_id: TaskId
    operator: None = None  # Not used

    __mermaid__ = """
    --8<-- [start:mermaid-task-props]
    <<OrbiterOperator>>
    imports: List[OrbiterRequirement]
    task_id: str
    **kwargs
    --8<-- [end:mermaid-task-props]
    """

    def _to_ast(self) -> ast.stmt:
        def prop(k):
            attr = getattr(self, k)
            return ast.Name(id=attr.__name__) if isinstance(attr, Callable) else attr

        # Figure out which operator we are talking about
        operator_names = [
            name
            for _import in self.imports
            for name in _import.names
            if "operator" in name.lower() or "sensor" in name.lower()
        ]
        if len(operator_names) != 1:
            raise ValueError(f"Expected exactly one operator name, got {operator_names}")
        [operator] = operator_names

        self_as_ast = py_assigned_object(
            to_task_id(self.task_id, ORBITER_TASK_SUFFIX),
            operator,
            **{k: prop(k) for k in ["task_id"] + sorted(self.__pydantic_extra__.keys()) if k and getattr(self, k)},
        )
        callable_props = [k for k in self.__pydantic_extra__.keys() if isinstance(getattr(self, k), Callable)]
        return (
            [py_function(getattr(self, prop)) for prop in callable_props] + [self_as_ast]
            if len(callable_props)
            else self_as_ast
        )


@validate_call
def to_task_id(task_id: str, assignment_suffix: str = "") -> str:
    # noinspection PyTypeChecker
    """General utiltty function - turns MyTaskId into my_task_id (or my_task_id_task suffix is `_task`)
    ```pycon
    >>> to_task_id("MyTaskId")
    'my_task_id'
    >>> to_task_id("MyTaskId", "_task")
    'my_task_id_task'
    >>> to_task_id("my_task_id_task", "_task")
    'my_task_id_task'

    ```
    :param task_id:
    :type task_id: str
    :param assignment_suffix: e.g. `_task` for `task_id_task = MyOperator(...)`
    :type assignment_suffix: str
    """
    task_id = clean_value(task_id)
    return task_id + (assignment_suffix if task_id[-len(assignment_suffix) :] != assignment_suffix else "")
