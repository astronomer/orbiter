from __future__ import annotations

import ast
from abc import ABC
from typing import Annotated, Union, Optional, List, Any, Set, Literal

from pydantic import field_validator, validate_call, Field

from orbiter.config import ORBITER_TASK_SUFFIX
from orbiter.ast_helper import (
    OrbiterASTBase,
    py_with,
    py_object,
    py_bitshift,
)
from orbiter.objects import OrbiterBase, ImportList, OrbiterRequirement
from orbiter.objects.operators.bash import OrbiterBashOperator
from orbiter.objects.operators.empty import OrbiterEmptyOperator
from orbiter.objects.operators.kubernetes_pod import OrbiterKubernetesPodOperator
from orbiter.objects.operators.livy import OrbiterLivyOperator
from orbiter.objects.operators.python import OrbiterPythonOperator, OrbiterDecoratedPythonOperator
from orbiter.objects.operators.smtp import OrbiterEmailOperator
from orbiter.objects.operators.sql import OrbiterSQLExecuteQueryOperator
from orbiter.objects.operators.ssh import OrbiterSSHOperator
from orbiter.objects.operators.win_rm import OrbiterWinRMOperator
from orbiter.objects.task import (
    TaskId,
    OrbiterTaskDependency,
    OrbiterTask,
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
    >>> OrbiterTaskGroup(task_group_id="foo").add_tasks([
    ...   OrbiterBashOperator(task_id="b", bash_command="b"),
    ...   OrbiterBashOperator(task_id="a", bash_command="a").add_downstream("b"),
    ... ]).add_downstream("c")
    with TaskGroup(group_id='foo') as foo:
        b_task = BashOperator(task_id='b', bash_command='b')
        a_task = BashOperator(task_id='a', bash_command='a')
        a_task >> b_task

    >>> render_ast(OrbiterTaskGroup(task_group_id="foo", downstream={"c"})._downstream_to_ast())
    'foo >> c_task'

    ```
    :param task_group_id: The id of the TaskGroup
    :type task_group_id: str
    :param tasks: The tasks in the TaskGroup
    :type tasks: Dict[str, OrbiterOperator | OrbiterTaskGroup]
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """

    __mermaid__ = """
       --8<-- [start:mermaid-op-props]
    task_group_id: str
    tasks: Dict[str, OrbiterOperator | OrbiterTaskGroup]
    add_downstream(str | List[str] | OrbiterTaskDependency)
    --8<-- [end:mermaid-op-props]
    """
    orbiter_type: Literal["OrbiterTaskGroup"] = "OrbiterTaskGroup"

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow",
            module="airflow.utils.task_group",
            names=["TaskGroup"],
        )
    ]
    task_group_id: TaskId
    tasks: "TasksType"
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
        if any(not (issubclass(type(i), OrbiterOperator) or isinstance(i, OrbiterTaskGroup)) for i in tasks.values()):
            raise TypeError(
                f"At least one item in tasks={tasks} is not a subclass of OrbiterOperator nor an OrbiterTaskGroup"
            )
        return tasks

    def add_tasks(self, tasks):
        from orbiter.objects.dag import _add_tasks

        return _add_tasks(self, tasks)

    def add_downstream(self, task_id: str | List[str] | OrbiterTaskDependency) -> "OrbiterTaskGroup":
        return task_add_downstream(self, task_id)

    def _downstream_to_ast(self):
        if not self.downstream:
            return
        elif len(self.downstream) == 1:
            (t,) = tuple(self.downstream)
            return py_bitshift(to_task_id(self.task_id), to_task_id(t, ORBITER_TASK_SUFFIX))
        else:
            return py_bitshift(
                to_task_id(self.task_id),
                sorted([to_task_id(t, ORBITER_TASK_SUFFIX) for t in self.downstream]),
            )

    def _to_ast(self) -> ast.stmt:
        """
        ```pycon
        >>> from orbiter.objects.operators.bash import OrbiterBashOperator
        >>> from orbiter.ast_helper import render_ast
        >>> # noinspection PyProtectedMember
        ... OrbiterTaskGroup(task_group_id="foo").add_tasks([
        ...     OrbiterBashOperator(task_id="a", bash_command="a").add_downstream("b"),
        ...     OrbiterBashOperator(task_id="b", bash_command="b")
        ... ])  # doctest: +NORMALIZE_WHITESPACE
        with TaskGroup(group_id='foo') as foo:
            a_task = BashOperator(task_id='a', bash_command='a')
            b_task = BashOperator(task_id='b', bash_command='b')
            a_task >> b_task

        ```
        """
        # noinspection PyProtectedMember
        return py_with(
            py_object("TaskGroup", group_id=self.task_group_id).value,
            [operator._to_ast() for operator in self.tasks.values()]
            + [downstream for operator in self.tasks.values() if (downstream := operator._downstream_to_ast())],
            self.task_group_id,
        )


# https://github.com/pydantic/pydantic/issues/8790
OrbiterTaskGroup.add_tasks = validate_call()(OrbiterTaskGroup.add_tasks)

# This needs to be **here**, specifically,
# to handle circular reference with OrbiterDAG
# and forward-reference with OrbiterTaskGroup
TaskType = Annotated[
    Union[
        OrbiterTask
        | OrbiterTaskGroup
        | OrbiterOperator
        | OrbiterEmptyOperator
        | OrbiterBashOperator
        | OrbiterLivyOperator
        | OrbiterPythonOperator
        | OrbiterDecoratedPythonOperator
        | OrbiterEmailOperator
        | OrbiterSQLExecuteQueryOperator
        | OrbiterKubernetesPodOperator
        | OrbiterSSHOperator
        | OrbiterWinRMOperator
    ],
    Field(discriminator="orbiter_type"),
]

TasksType = Annotated[Optional[dict[str, TaskType]], Field(default_factory=dict)]
