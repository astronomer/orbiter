from __future__ import annotations

import ast
from abc import ABC
from typing import Set, List, ClassVar, Annotated, Callable

from loguru import logger
from pydantic import AfterValidator, BaseModel

from orbiter.ast_helper import OrbiterASTBase, py_bitshift, py_function
from orbiter.ast_helper import py_assigned_object
from orbiter.objects import ImportList
from orbiter.objects import OrbiterBase
from orbiter.objects.pool import OrbiterPool
from orbiter import to_task_id

__mermaid__ = """
--8<-- [start:mermaid-dag-relationships]
OrbiterOperator --> "many" OrbiterRequirement
OrbiterOperator --> "one" OrbiterPool
OrbiterOperator --> "many" OrbiterConnection
OrbiterOperator --> "many" OrbiterVariable
OrbiterOperator --> "many" OrbiterEnvVar
OrbiterOperator --> "many" OrbiterTaskDependency
--8<-- [end:mermaid-dag-relationships]

--8<-- [start:mermaid-task-relationships]
OrbiterOperator "implements" <|-- OrbiterTask
OrbiterOperator --> "many" OrbiterCallback
--8<-- [end:mermaid-task-relationships]
"""

RenderAttributes = ClassVar[List[str]]
TaskId = Annotated[str, AfterValidator(lambda t: to_task_id(t))]


def task_add_downstream(
    self, task_id: str | List[str] | OrbiterTaskDependency
) -> "OrbiterOperator" | "OrbiterTaskGroup":  # noqa: F821
    """
    Add a downstream task dependency
    """
    if isinstance(task_id, OrbiterTaskDependency):
        task_dependency = task_id
        if task_dependency.task_id != self.task_id:
            raise ValueError(
                f"task_dependency={task_dependency} has a different task_id than {self.task_id}"
            )
        self.downstream.add(task_dependency)
        return self

    if not len(task_id):
        return self

    if len(task_id) == 1:
        task_id = task_id[0]
    downstream_task_id = (
        [to_task_id(t) for t in task_id]
        if isinstance(task_id, list)
        else to_task_id(task_id)
    )
    logger.debug(f"Adding downstream {downstream_task_id} to {self.task_id}")
    self.downstream.add(
        OrbiterTaskDependency(task_id=self.task_id, downstream=downstream_task_id)
    )
    return self


class OrbiterTaskDependency(OrbiterASTBase, BaseModel, extra="forbid"):
    """Represents a task dependency, which is added to either an
    [`OrbiterOperator`][orbiter.objects.task.OrbiterOperator]
    or an [`OrbiterTaskGroup`][orbiter.objects.task_group.OrbiterTaskGroup].

    Can take a single downstream `task_id`
    ```pycon
    >>> OrbiterTaskDependency(task_id="task_id", downstream="downstream")
    task_id_task >> downstream_task

    ```

    or a list of downstream `task_ids`
    ```pycon
    >>> OrbiterTaskDependency(task_id="task_id", downstream=["a", "b"])
    task_id_task >> [a_task, b_task]

    ```

    :param task_id: The task_id for the operator
    :type task_id: str
    :param downstream: downstream tasks
    :type downstream: str | List[str]
    """

    # --8<-- [start:mermaid-td-props]
    task_id: TaskId
    downstream: TaskId | List[TaskId]
    # --8<-- [end:mermaid-td-props]

    def _to_ast(self):
        if isinstance(self.downstream, str):
            return py_bitshift(
                to_task_id(self.task_id, "_task"), to_task_id(self.downstream, "_task")
            )
        elif isinstance(self.downstream, list):
            return py_bitshift(
                to_task_id(self.task_id, "_task"),
                [to_task_id(t, "_task") for t in self.downstream],
            )


class OrbiterOperator(OrbiterASTBase, OrbiterBase, ABC, extra="allow"):
    """
    **Abstract class** representing a
    [Task in Airflow](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html#operators),
    must be subclassed (such as [`OrbiterBashOperator`][orbiter.objects.operators.bash.OrbiterBashOperator])

    Instantiation/inheriting:
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
    >>> foo.add_downstream("downstream").downstream
    {task_id_task >> downstream_task}

    ```

    Adding multiple downstream tasks:
    ```pycon
    >>> sorted(list(foo.add_downstream(["a", "b"]).downstream))
    [task_id_task >> [a_task, b_task], task_id_task >> downstream_task]

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
    :param task_id: The task_id for the operator
    :param trigger_rule: optional, conditions under which to start the task
      https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#trigger-rules
    :param pool: optional, name of the pool to use
      https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html#pools
    :param pool_slots: optional, slots for this task to take in the pool
    :param orbiter_pool: optional, OrbiterPool object
    :param operator: operator name
    :param downstream: downstream tasks
    """

    imports: ImportList

    operator: str
    task_id: TaskId

    trigger_rule: str | None = None

    pool: str | None = None
    pool_slots: int | None = None
    orbiter_pool: OrbiterPool | None = None
    downstream: Set[OrbiterTaskDependency] = set()

    render_attributes: RenderAttributes = [
        "task_id",
        "pool",
        "pool_slots",
        "trigger_rule",
    ]

    __mermaid__ = """
    --8<-- [start:mermaid-op-props]
    imports: List[OrbiterRequirement]
    operator: str
    task_id: str
    pool: str | None
    pool_slots: int | None
    trigger_rule: str | None
    downstream: Set[OrbiterTaskDependency]
    add_downstream(str | List[str] | OrbiterTaskDependency)
    --8<-- [end:mermaid-op-props]
    """

    def add_downstream(
        self, task_id: str | List[str] | OrbiterTaskDependency
    ) -> "OrbiterOperator":
        return task_add_downstream(self, task_id)

    def _to_ast(self) -> ast.stmt:
        def prop(k):
            attr = getattr(self, k, None) or getattr(self.model_extra, k, None)
            return ast.Name(id=attr.__name__) if isinstance(attr, Callable) else attr

        # foo = Bar(x=x,y=y, z=z)
        return py_assigned_object(
            to_task_id(self.task_id, "_task"),
            self.operator,
            **{k: prop(k) for k in self.render_attributes if k and getattr(self, k)},
            **{k: prop(k) for k in (self.model_extra.keys() or [])},
        )


class OrbiterTask(OrbiterOperator, extra="allow"):
    """
    A generic Airflow [`OrbiterOperator`][orbiter.objects.task.OrbiterOperator] that can be instantiated directly.

    The operator that is instantiated is inferred from the `imports` field.

    The first `*Operator` or `*Sensor` import is used.

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

    :param task_id: The task_id for the operator
    :param imports: List of requirements for the operator (operator is inferred from first "XYZOperator")
    :param **kwargs: Any other keyword arguments to be passed to the operator
    """

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
            raise ValueError(
                f"Expected exactly one operator name, got {operator_names}"
            )
        [operator] = operator_names

        self_as_ast = py_assigned_object(
            to_task_id(self.task_id, "_task"),
            operator,
            **{
                k: prop(k)
                for k in ["task_id"] + sorted(self.__pydantic_extra__.keys())
                if k and getattr(self, k)
            },
        )
        callable_props = [
            k
            for k in self.__pydantic_extra__.keys()
            if isinstance(getattr(self, k), Callable)
        ]
        return (
            [py_function(getattr(self, prop)) for prop in callable_props]
            + [self_as_ast]
            if len(callable_props)
            else self_as_ast
        )
