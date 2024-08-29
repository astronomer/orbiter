from __future__ import annotations

import ast
from abc import ABC
from typing import List, Any, Set

from pydantic import field_validator

from orbiter.config import ORBITER_TASK_SUFFIX
from orbiter.ast_helper import (
    OrbiterASTBase,
    py_with,
    py_object,
    py_bitshift,
)
from orbiter.objects import OrbiterBase, ImportList, OrbiterRequirement
from orbiter.objects.task import (
    TaskId,
    OrbiterTaskDependency,
    OrbiterOperator,
    task_add_downstream,
    to_task_id,
)

__mermaid__ = """
--8<-- [start:mermaid-dag-relationships]
OrbiterTaskGroup --> "many" OrbiterRequirement
--8<-- [end:mermaid-dag-relationships]

--8<-- [start:mermaid-task-relationships]
--8<-- [end:mermaid-task-relationships]
"""


class OrbiterTaskGroup(OrbiterASTBase, OrbiterBase, ABC, extra="forbid"):
    """
    Represents a [TaskGroup](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#taskgroups)
    in Airflow, which contains multiple tasks

    ```pycon
    >>> from orbiter.objects.operators.bash import OrbiterBashOperator
    >>> from orbiter.ast_helper import render_ast
    >>> OrbiterTaskGroup(task_group_id="foo", tasks=[
    ...   OrbiterBashOperator(task_id="b", bash_command="b"),
    ...   OrbiterBashOperator(task_id="a", bash_command="a").add_downstream("b"),
    ... ], downstream={"c"})
    with TaskGroup(group_id='foo') as foo:
        b_task = BashOperator(task_id='b', bash_command='b')
        a_task = BashOperator(task_id='a', bash_command='a')
        a_task >> b_task

    >>> render_ast(OrbiterTaskGroup(task_group_id="foo", tasks=[], downstream={"c"})._downstream_to_ast())
    'foo >> c_task'

    ```

    :param task_group_id: The id of the TaskGroup
    :type task_group_id: str
    :param tasks: The tasks in the TaskGroup
    :type tasks: List[OrbiterOperator | OrbiterTaskGroup]
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """

    __mermaid__ = """
       --8<-- [start:mermaid-op-props]
    task_group_id: str
    tasks: List[OrbiterOperator | OrbiterTaskGroup]
    add_downstream(str | List[str] | OrbiterTaskDependency)
    --8<-- [end:mermaid-op-props]
    """

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow",
            module="airflow.utils.task_group",
            names=["TaskGroup"],
        )
    ]
    task_group_id: TaskId
    tasks: List[Any]
    downstream: Set[str] = set()

    @property
    def task_id(self):
        # task_id property, so it can be treated like an OrbiterOperator more easily
        return self.task_group_id

    @task_id.setter
    def task_id(self, value):
        # task_id property, so it can be treated like an OrbiterOperator more easily
        self.task_group_id = value

    # noinspection PyNestedDecorators
    @field_validator("tasks")
    @classmethod
    def ensure_tasks(cls, tasks: Any):
        if any(
            not (
                issubclass(type(i), OrbiterOperator) or isinstance(i, OrbiterTaskGroup)
            )
            for i in tasks
        ):
            raise TypeError(
                f"At least one item in tasks={tasks} is not a subclass of OrbiterOperator nor an OrbiterTaskGroup"
            )
        return tasks

    def add_downstream(
        self, task_id: str | List[str] | OrbiterTaskDependency
    ) -> "OrbiterTaskGroup":
        return task_add_downstream(self, task_id)

    def _downstream_to_ast(self):
        if not self.downstream:
            return
        elif len(self.downstream) == 1:
            (t,) = tuple(self.downstream)
            return py_bitshift(
                to_task_id(self.task_id), to_task_id(t, ORBITER_TASK_SUFFIX)
            )
        else:
            return py_bitshift(
                to_task_id(self.task_id),
                sorted([to_task_id(t, ORBITER_TASK_SUFFIX) for t in self.downstream]),
            )

    def _to_ast(self) -> ast.stmt:
        # noinspection PyProtectedMember
        return py_with(
            py_object("TaskGroup", group_id=self.task_group_id).value,
            [operator._to_ast() for operator in self.tasks]
            + [operator._downstream_to_ast() for operator in self.tasks],
            self.task_group_id,
        )
