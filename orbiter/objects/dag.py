from __future__ import annotations

import ast
from datetime import datetime, timedelta
from functools import reduce
from pathlib import Path
from typing import Annotated, Any, Dict, Iterable, List, Callable, ClassVar, TYPE_CHECKING

from loguru import logger

try:
    from typing import Self  # py3.11
except ImportError:
    from typing_extensions import Self

from pydantic_extra_types.pendulum_dt import DateTime
from pydantic import AfterValidator, validate_call

from orbiter import clean_value
from orbiter.ast_helper import OrbiterASTBase, py_object, py_with
from orbiter.objects import ImportList, OrbiterBase, CALLBACK_KEYS
from orbiter.objects.callbacks.callback_type import CallbackType
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.task import OrbiterOperator
from orbiter.objects.tasks_parent_shared_utils import _add_tasks, _get_task_dependency_parent
from orbiter.objects.timetables import TimetableType

if TYPE_CHECKING:
    from orbiter.objects.task import OrbiterOperator
    from orbiter.objects.task_group import OrbiterTaskGroup
    from orbiter.objects.task_group import TasksType, TaskType


__mermaid__ = """
--8<-- [start:mermaid-project-relationships]
OrbiterDAG --> "many" OrbiterInclude
OrbiterDAG --> "many" OrbiterConnection
OrbiterDAG --> "many" OrbiterEnvVar
OrbiterDAG --> "many" OrbiterRequirement
OrbiterDAG --> "many" OrbiterVariable
--8<-- [end:mermaid-project-relationships]

--8<-- [start:mermaid-dag-relationships]
OrbiterDAG --> "many" OrbiterOperator
OrbiterDAG --> "many" OrbiterTaskGroup
OrbiterDAG --> "many" OrbiterRequirement
OrbiterDAG --> "many" OrbiterCallback
--8<-- [end:mermaid-dag-relationships]
"""

DagId = Annotated[str, AfterValidator(lambda d: to_dag_id(d))]


def _get_imports_recursively(
    tasks: Iterable[OrbiterOperator | OrbiterTaskGroup],
) -> List[OrbiterRequirement]:
    """

    >>> from orbiter.objects.task import OrbiterTask
    >>> from orbiter.objects.task_group import OrbiterTaskGroup
    >>> from orbiter.objects.callbacks import OrbiterCallback
    >>> _get_imports_recursively(
    ...     [
    ...         OrbiterTask(task_id="foo", imports=[OrbiterRequirement(names=["foo"])]),
    ...         OrbiterTaskGroup(
    ...             task_group_id="bar",
    ...             imports=[OrbiterRequirement(names=["bar"])],
    ...             tasks={
    ...                 "baz": OrbiterTask(
    ...                     task_id="baz",
    ...                     imports=[OrbiterRequirement(names=["baz"])],
    ...                     on_failure_callback=OrbiterCallback(
    ...                         imports=[OrbiterRequirement(names=["qux"])],
    ...                         function="qux",
    ...                     ),
    ...                 )
    ...             },
    ...         ),
    ...     ]
    ... )
    ... # doctest: +ELLIPSIS
    [OrbiterRequirement(...names=[bar]...names=[baz]...names=[foo]...names=[qux]...]

    """
    imports = set()
    for task in tasks:
        # Add task imports
        imports |= set(task.imports)

        def reduce_imports_from_callback(old, item):
            try:
                # Look for on_failure_callback
                task_props = (getattr(task, "model_extra", {}) or {}) | (getattr(task, "__dict__", {}) or {})
                callback = task_props.get(item)
                # get imports from callback, merge them all
                return old | set(getattr(callback, "imports"))
            except (AttributeError, KeyError):
                return old

        imports |= reduce(reduce_imports_from_callback, CALLBACK_KEYS, set())
        if hasattr(task, "tasks"):
            # descend, for a task group
            # noinspection PyUnresolvedReferences
            imports |= set(_get_imports_recursively(task.tasks.values()))
    return list(sorted(imports, key=str))


def dereference_downstream(
    self: "OrbiterDAG | OrbiterTaskGroup", root_orbiter_dag: "OrbiterDAG | None" = None
) -> "OrbiterDAG | OrbiterTaskGroup":
    """Turn "downstream" references into the actual `OrbiterOperator` objects
    via `_dereferenced_downstream`. Recursively descends into task groups
    """
    root_orbiter_dag = root_orbiter_dag or self

    def _find_dereferenced_task(task_dependency: str) -> TaskType | str | None:
        if (parent := root_orbiter_dag.get_task_dependency_parent(task_dependency)) is not None and (
            child := parent.tasks.get(task_dependency)
        ) is not None:
            return child
        else:
            logger.warning(f"Unable to find {task_dependency=} in {root_orbiter_dag.dag_id=}.")
            return task_dependency

    for task_id, task in self.tasks.items():
        task._dereferenced_downstream = {
            _find_dereferenced_task(task_dependency) for task_dependency in task.downstream
        }
        if hasattr(task, "tasks"):
            dereference_downstream(task, (root_orbiter_dag or self))
    return self


class OrbiterDAG(OrbiterASTBase, OrbiterBase, extra="allow"):
    """Represents an Airflow [DAG](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html),
    with its tasks and dependencies.

    Renders to a `.py` file in the `/dags` folder

    :param file_path: File path of the DAG, relative to the `/dags` folder
        (`filepath=my_dag.py` would render to `dags/my_dag.py`)
    :type file_path: str
    :param dag_id: The `dag_id`. Must be unique and snake_case. Good practice is to set `dag_id` == `file_path`
    :type dag_id: str
    :param schedule: The schedule for the DAG. Defaults to None (only runs when manually triggered)
    :type schedule: str | OrbiterTimetable, optional
    :param catchup: Whether to catchup runs from the `start_date` to now, on first run. Defaults to False
    :type catchup: bool, optional
    :param start_date: The start date for the DAG. Defaults to Unix Epoch
    :type start_date: DateTime, optional
    :param tags: Tags for the DAG, used for sorting and filtering in the Airflow UI
    :type tags: List[str], optional
    :param default_args: Default arguments for any tasks in the DAG
    :type default_args: Dict[str, Any], optional
    :param params: Params for the DAG
    :type params: Dict[str, Any], optional
    :param doc_md: Documentation for the DAG with markdown support
    :type doc_md: str, optional
    :param kwargs: Additional keyword arguments to pass to the DAG
    :type kwargs: dict, optional
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    imports: List[OrbiterRequirement]
    file_path: str
    dag_id: str
    schedule: str | OrbiterTimetable | None
    catchup: bool
    start_date: DateTime
    tags: List[str]
    default_args: Dict[str, Any]
    params: Dict[str, Any]
    doc_md: str | None
    tasks: Dict[str, OrbiterOperator]
    kwargs: dict
    orbiter_kwargs: dict
    orbiter_conns: Set[OrbiterConnection]
    orbiter_vars: Set[OrbiterVariable]
    orbiter_env_vars: Set[OrbiterEnvVar]
    orbiter_includes: Set[OrbiterInclude]
    on_failure_callback: OrbiterCallback
    on_success_callback: OrbiterCallback
    on_retry_callback: OrbiterCallback
    on_skipped_callback: OrbiterCallback
    on_execute_callback: OrbiterCallback
    sla_miss_callback: OrbiterCallback
    --8<-- [end:mermaid-props]
    """

    # noinspection PyTypeHints
    imports: ImportList = [
        OrbiterRequirement(package="apache-airflow", module="airflow", names=["DAG"]),
        OrbiterRequirement(package="pendulum", module="pendulum", names=["DateTime", "Timezone"]),
    ]
    file_path: str | Path

    dag_id: DagId
    schedule: str | timedelta | TimetableType | None = None
    catchup: bool | None = None
    start_date: datetime | DateTime | None = None
    tags: List[str] | None = None
    default_args: Dict[str, Any] | None = None
    params: Dict[str, Any] | None = None
    doc_md: str | None = None

    on_success_callback: CallbackType
    on_failure_callback: CallbackType
    on_retry_callback: CallbackType
    on_execute_callback: CallbackType
    on_skipped_callback: CallbackType
    sla_miss_callback: CallbackType

    tasks: TasksType

    render_attributes: ClassVar[List[str]] = [
        "dag_id",
        "schedule",
        "start_date",
        "catchup",
        "tags",
        "default_args",
        "params",
        "doc_md",
    ] + CALLBACK_KEYS

    def repr(self):
        return (
            f"OrbiterDAG("
            f"dag_id={self.dag_id}, "
            f"schedule={self.schedule}, "
            f"start_date={self.start_date}, "
            f"catchup={self.catchup})"
        )

    # noinspection t,D
    def __add__(self, other):
        if other.tasks:
            for task in other.tasks.values():
                self.add_tasks(task)
        if other.orbiter_conns:
            for conn in other.orbiter_conns:
                self.orbiter_conns.add(conn)
        if other.orbiter_vars:
            for var in other.orbiter_vars:
                self.orbiter_vars.add(var)
        if other.orbiter_env_vars:
            for env_var in other.orbiter_env_vars:
                self.orbiter_env_vars.add(env_var)
        if other.orbiter_includes:
            for include in other.orbiter_includes:
                self.orbiter_includes.add(include)
        if other.model_extra:
            for key in other.model_extra.keys():
                self.model_extra[key] = self.model_extra[key] or other.model_extra[key]
        for key in self.render_attributes:
            setattr(self, key, getattr(self, key) or getattr(other, key))
        return self

    def add_tasks(self, tasks) -> Self:
        return _add_tasks(self, tasks)

    def get_task_dependency_parent(self, task_dependency) -> Self | None:
        return _get_task_dependency_parent(self, task_dependency)

    def _dag_to_ast(self) -> ast.Expr:
        """
        Returns the `DAG(...)` object.
        OrbiterDAG._to_ast will handle the rest (like imports, the context manager, tasks, and task dependencies)

        ```pycon
        >>> from orbiter.ast_helper import render_ast
        >>> render_ast(OrbiterDAG(dag_id="dag_id", file_path="")._dag_to_ast())
        "DAG(dag_id='dag_id')"

        ```

        ```pycon
        >>> render_ast(OrbiterDAG(
        ...    dag_id="dag_id",
        ...    file_path="",
        ...    default_args=None,
        ...    params={},
        ...    schedule="@hourly",
        ...    start_date=datetime(2000, 1, 1),
        ...    description="foo",
        ... )._dag_to_ast())
        "DAG(dag_id='dag_id', schedule='@hourly', start_date=datetime.datetime(2000, 1, 1, 0, 0), params={}, description='foo')"

        >>> from orbiter.objects.callbacks.smtp import OrbiterSmtpNotifierCallback
        >>> render_ast(
        ...   OrbiterDAG(file_path="", dag_id="foo", catchup=False, on_retry_callback=OrbiterSmtpNotifierCallback(to="")
        ... )._dag_to_ast())
        "DAG(dag_id='foo', catchup=False, on_retry_callback=send_smtp_notification(from_email=None, smtp_conn_id='SMTP'))"

        ```
        :return: `DAG(...)` as an ast.Expr
        """  # noqa: E501

        def prop(k):
            attr = getattr(self, k, None)
            if attr is None:
                # Try model_extra, if we couldn't find it on the main object
                attr = getattr(self.model_extra, k, None)
            return ast.Name(id=attr.__name__) if isinstance(attr, Callable) else attr

        index_map = {v: i for i, v in enumerate(self.render_attributes)}
        rendered_params = {k: prop(k) for k in self.render_attributes if (getattr(self, k) is not None)}
        extra_params = {k: prop(k) for k in (self.model_extra.keys() or [])}
        return py_object(
            name="DAG",
            **dict(
                sorted(
                    rendered_params.items(),
                    key=lambda k_v: index_map[k_v[0]],
                )
            ),
            **extra_params,
        )

    # noinspection PyProtectedMember,D
    def _to_ast(self) -> List[ast.stmt]:
        """
        Renders the DAG to an AST, including imports, tasks, and task dependencies

        ```pycon
        >>> from orbiter.objects.task_group import OrbiterTaskGroup
        >>> from orbiter.objects.operators.bash import OrbiterBashOperator
        >>> from orbiter.ast_helper import render_ast
        >>> # noinspection PyProtectedMember
        ... OrbiterDAG(dag_id="foo", file_path="").add_tasks([
        ...     OrbiterBashOperator(task_id="a", bash_command="a").add_downstream("b"),
        ...     OrbiterTaskGroup(task_group_id="b").add_tasks([
        ...         OrbiterBashOperator(task_id="c", bash_command="c").add_downstream("d"),
        ...         OrbiterBashOperator(task_id="d", bash_command="d")
        ...     ]).add_downstream("e"),
        ...     OrbiterBashOperator(task_id="e", bash_command="e")
        ... ])  # doctest: +NORMALIZE_WHITESPACE
        from airflow import DAG
        from airflow.operators.bash import BashOperator
        from airflow.utils.task_group import TaskGroup
        with DAG(dag_id='foo'):
            a_task = BashOperator(task_id='a', bash_command='a')
            with TaskGroup(group_id='b') as b:
                c_task = BashOperator(task_id='c', bash_command='c')
                d_task = BashOperator(task_id='d', bash_command='d')
                c_task >> d_task
            e_task = BashOperator(task_id='e', bash_command='e')
            a_task >> b
            b >> e_task

        ```
        """
        from orbiter.objects.timetables.timetable import OrbiterTimetable

        def dedupe_callable(ast_collection):
            items = []
            for item in ast_collection:
                if isinstance(item, list):
                    items.extend(item)
                else:
                    items.append(item)

            seen = set()
            for item in items:
                if isinstance(item, ast.FunctionDef):
                    if item.name in seen:
                        continue
                    seen.add(item.name)
                yield item

        # Turn "downstream" references into the actual OrbiterOperator objects via _dereferenced_downstream
        # Recursively descends into task groups
        dereference_downstream(self)

        # DAG Imports, e.g. `from airflow import DAG`
        # Task/TaskGroup Imports, e.g. `from airflow.operators.bash import BashOperator`
        pre_imports = list(
            # Ignore the pendulum import if start_date is None
            set(i for i in self.imports if not (i.package == "pendulum" and self.start_date is None))
            # Get imports from tasks, and descend into task groups
            | set(_get_imports_recursively(self.tasks.values()))
            # Get imports from Schedule, if it's a timetable
            | (set(self.schedule.imports) if isinstance(self.schedule, OrbiterTimetable) else set())
            | reduce(
                # Look for e.g. on_failure_callback, get imports, merge them all
                lambda old, item: old | set(getattr(getattr(self, item, {}), "imports", set())),
                CALLBACK_KEYS,
                set(),
            )
        )

        imports = [i._to_ast() for i in sorted(pre_imports)]

        # foo = BashOperator(...)
        task_definitions = list(dedupe_callable([task._to_ast() for task in self.tasks.values()]))

        # foo >> bar
        task_dependencies = [
            task._downstream_to_ast() for task in sorted(self.tasks.values()) if task._downstream_to_ast()
        ]

        # with DAG(...) as dag:
        with_dag = py_with(self._dag_to_ast().value, body=task_definitions + task_dependencies)
        return [
            *imports,
            with_dag,
        ]


@validate_call
def to_dag_id(dag_id: str) -> str:
    return clean_value(dag_id)


# This needs to be here, specifically after OrbiterDAG is defined
# to avoid circular imports
from orbiter.objects.task_group import TasksType  # noqa: E402

OrbiterDAG.model_rebuild()
