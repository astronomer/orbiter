from __future__ import annotations

import ast
from typing import Annotated, List, TYPE_CHECKING

from loguru import logger
from pydantic import AfterValidator, validate_call

from orbiter.ast_helper import py_bitshift
from orbiter.config import ORBITER_TASK_SUFFIX

if TYPE_CHECKING:
    from orbiter.objects.task import OrbiterOperator, OrbiterTaskDependency
    from orbiter.objects.task_group import OrbiterTaskGroup, TaskType

TaskId = Annotated[str, AfterValidator(lambda t: to_task_id(t))]


def downstream_to_ast(self: OrbiterTaskGroup | OrbiterOperator) -> List[ast.stmt] | None:
    downstream: set[TaskType] | set[str] = self._dereferenced_downstream or self.downstream
    if not downstream or not len(downstream):
        return None
    elif len(downstream) == 1:
        (t,) = tuple(downstream)
        return py_bitshift(
            self.get_rendered_task_id(),
            to_task_id(getattr(t, "task_id", t), ORBITER_TASK_SUFFIX)
            if isinstance(t, str)
            else t.get_rendered_task_id(),
        )
    else:
        return py_bitshift(
            self.get_rendered_task_id(),
            sorted(
                [
                    to_task_id(getattr(t, "task_id", t), ORBITER_TASK_SUFFIX)
                    if isinstance(t, str)
                    else t.get_rendered_task_id()
                    for t in downstream
                ]
            ),
        )


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
    from orbiter.objects.task import OrbiterTaskDependency

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


@validate_call
def to_task_id(task_id: str, assignment_suffix: str = "") -> str:
    # noinspection PyTypeChecker
    """General utility function - turns MyTaskId into my_task_id (or my_task_id_task suffix is `_task`)
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
    from orbiter import clean_value

    task_id = clean_value(task_id)
    return task_id + (assignment_suffix if task_id[-len(assignment_suffix) :] != assignment_suffix else "")
