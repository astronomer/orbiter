from __future__ import annotations

import ast
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Dict, Iterable, List

from pendulum import DateTime
from pydantic import AfterValidator, validate_call

from orbiter.ast_helper import OrbiterASTBase, py_object, py_with
from orbiter.objects import ImportList, OrbiterBase
from orbiter.objects.callbacks import OrbiterCallback
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.task import OrbiterOperator
from orbiter.objects.task_group import OrbiterTaskGroup
from orbiter.objects.timetables import OrbiterTimetable
from orbiter import to_dag_id

__mermaid__ = """
--8<-- [start:mermaid-project-relationships]
OrbiterDAG --> "many" OrbiterInclude
OrbiterDAG --> "many" OrbiterConnection
OrbiterDAG --> "many" OrbiterEnvVar
OrbiterDAG --> "many" OrbiterPool
OrbiterDAG --> "many" OrbiterRequirement
OrbiterDAG --> "many" OrbiterVariable
--8<-- [end:mermaid-project-relationships]

--8<-- [start:mermaid-dag-relationships]
OrbiterDAG "via schedule" --> OrbiterTimetable
OrbiterDAG --> "many" OrbiterOperator
OrbiterDAG --> "many" OrbiterTaskGroup
OrbiterDAG --> "many" OrbiterRequirement
--8<-- [end:mermaid-dag-relationships]
"""

DagId = Annotated[str, AfterValidator(lambda d: to_dag_id(d))]


def _get_imports_recursively(
    tasks: Iterable[OrbiterOperator | OrbiterTaskGroup],
) -> List[OrbiterRequirement]:
    imports = []
    extra_attributes_imports = []
    for task in tasks:
        for callback in [
            callback
            for callback in [
                ((task.__dict__ or {}) | (task.model_extra or {})).get(
                    "on_failure_callback"
                ),
                ((task.__dict__ or {}) | (task.model_extra or {})).get(
                    "on_success_callback"
                ),
            ]
            if callback
        ]:
            callback: OrbiterCallback
            extra_attributes_imports.extend(callback.imports)

        imports.extend(
            task.imports
            + extra_attributes_imports
            + _get_imports_recursively(task.tasks)
            if isinstance(task, OrbiterTaskGroup)
            else task.imports + extra_attributes_imports
        )
    return imports


class OrbiterDAG(OrbiterASTBase, OrbiterBase, extra="forbid"):
    """Represents an Airflow
    [DAG](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html),
    with its tasks and dependencies. Renders to a `.py` file in the `/dags` folder

    :param file_path: file path of the DAG
    :type file_path: str
    :param dag_id: The DAG ID
    :type dag_id: str
    :param schedule: The schedule interval for the DAG
    :type schedule: str | OrbiterTimetable | None
    :param catchup: Whether to catch up on missed intervals
    :type catchup: bool
    :param start_date: The start date for the DAG
    :type start_date: DateTime
    :param default_args: Default arguments for the DAG
    :type default_args: Dict[str, Any]
    :param params: Params for the DAG
    :type params: Dict[str, Any]
    :param doc_md: Markdown documentation for the DAG
    :type doc_md: str
    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    imports: List[OrbiterRequirement]
    file_path: str
    dag_id: str
    schedule: str | OrbiterTimetable | None
    catchup: bool
    start_date: DateTime
    default_args: Dict[str, Any]
    params: Dict[str, Any]
    doc_md: str | None
    tasks: Dict[str, OrbiterOperator]
    orbiter_kwargs: dict
    orbiter_conns: Set[OrbiterConnection]
    orbiter_vars: Set[OrbiterVariable]
    orbiter_env_vars: Set[OrbiterEnvVar]
    orbiter_includes: Set[OrbiterInclude]
    --8<-- [end:mermaid-props]

    # removed to simplify the diagram
    add_tasks(OrbiterOperator | Iterable[OrbiterOperator])
    """

    imports: ImportList = [
        OrbiterRequirement(package="apache-airflow", module="airflow", names=["DAG"]),
        OrbiterRequirement(
            package="pendulum", module="pendulum", names=["DateTime", "Timezone"]
        ),
    ]
    file_path: str | Path

    dag_id: DagId
    schedule: str | OrbiterTimetable | None = None
    catchup: bool = False
    start_date: DateTime | datetime = DateTime(1970, 1, 1)
    tags: List[str] = None
    default_args: Dict[str, Any] = dict()
    params: Dict[str, Any] = dict()
    doc_md: str | None = None

    tasks: Dict[str, OrbiterOperator | OrbiterTaskGroup] = dict()

    nullable_attributes: List[str] = ["catchup", "schedule"]
    render_attributes: List[str] = [
        "dag_id",
        "schedule",
        "start_date",
        "catchup",
        "tags",
        "default_args",
        "params",
        "doc_md",
    ]

    def __repr__(self):
        return (
            f"OrbiterDAG("
            f"dag_id={self.dag_id}, "
            f"schedule={self.schedule}, "
            f"start_date={self.start_date}, "
            f"catchup={self.catchup})"
        )

    def _dag_to_ast(self) -> ast.Expr:
        """
        Returns the `DAG(...)` object.
        OrbiterDAG._to_ast will handle the rest (like imports, the context manager, tasks, and task dependencies)

        ```pycon
        >>> from orbiter.ast_helper import render_ast
        >>> render_ast(OrbiterDAG(dag_id="dag_id", file_path="")._dag_to_ast())
        "DAG(dag_id='dag_id', schedule=None, start_date=DateTime(1970, 1, 1, 0, 0, 0), catchup=False)"

        ```

        ```pycon
        >>> render_ast(OrbiterDAG(
        ...    dag_id="dag_id",
        ...    file_path="",
        ...    default_args={},
        ...    params={},
        ...    schedule="@hourly",
        ...    start_date=datetime(2000, 1, 1)
        ...    )._dag_to_ast())
        "DAG(dag_id='dag_id', schedule='@hourly', start_date=datetime.datetime(2000, 1, 1, 0, 0), catchup=False)"

        ```
        :return: `DAG(...)` as an ast.Expr
        """
        index_map = {v: i for i, v in enumerate(self.render_attributes)}
        rendered_params = {
            k: getattr(self, k)
            for k in self.render_attributes
            if getattr(self, k) or k in self.nullable_attributes
        }
        return py_object(
            name="DAG",
            **dict(
                sorted(
                    rendered_params.items(),
                    key=lambda pair: index_map[pair[0]],
                )
            ),
        )

    def add_tasks(
        self,
        tasks: (
            OrbiterOperator
            | OrbiterTaskGroup
            | Iterable[OrbiterOperator | OrbiterTaskGroup]
        ),
    ) -> "OrbiterDAG":
        """
        Add one or more [`OrbiterOperators`][orbiter.objects.task.OrbiterOperator] to the DAG

        ```pycon
        >>> from orbiter.objects.operators.empty import OrbiterEmptyOperator
        >>> OrbiterDAG(file_path="", dag_id="foo").add_tasks(OrbiterEmptyOperator(task_id="bar")).tasks
        {'bar': bar_task = EmptyOperator(task_id='bar')}

        >>> OrbiterDAG(file_path="", dag_id="foo").add_tasks([OrbiterEmptyOperator(task_id="bar")]).tasks
        {'bar': bar_task = EmptyOperator(task_id='bar')}

        ```

        !!! tip

            Validation requires a `OrbiterTaskGroup`, `OrbiterOperator` (or subclass), or list of either to be passed
            ```pycon
            >>> # noinspection PyTypeChecker
            ... OrbiterDAG(file_path="", dag_id="foo").add_tasks("bar")
            ... # doctest: +IGNORE_EXCEPTION_DETAIL
            Traceback (most recent call last):
            pydantic_core._pydantic_core.ValidationError: ...
            >>> # noinspection PyTypeChecker
            ... OrbiterDAG(file_path="", dag_id="foo").add_tasks(["bar"])
            ... # doctest: +IGNORE_EXCEPTION_DETAIL
            Traceback (most recent call last):
            pydantic_core._pydantic_core.ValidationError: ...

            ```
        :param tasks: List of [OrbiterOperator][orbiter.objects.task.OrbiterOperator], or OrbiterTaskGroup or subclass
        :type tasks: OrbiterOperator | OrbiterTaskGroup | Iterable[OrbiterOperator | OrbiterTaskGroup]
        :return: self
        :rtype: OrbiterProject
        """
        if (
            isinstance(tasks, OrbiterOperator)
            or isinstance(tasks, OrbiterTaskGroup)
            or issubclass(type(tasks), OrbiterOperator)
        ):
            tasks = [tasks]

        for task in tasks:
            try:
                task_id = getattr(task, "task_id", None) or getattr(
                    task, "task_group_id"
                )
            except AttributeError:
                raise AttributeError(
                    f"Task {task} does not have a task_id or task_group_id attribute"
                )
            self.tasks[task_id] = task
        return self

    # noinspection PyProtectedMember
    def _to_ast(self) -> List[ast.stmt]:
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

        # DAG Imports, e.g. `from airflow import DAG`
        # Task/TaskGroup Imports, e.g. `from airflow.operators.bash import BashOperator`
        pre_imports = list(
            set(self.imports)
            | set(_get_imports_recursively(self.tasks.values()))
            | (
                set(self.schedule.imports)
                if isinstance(self.schedule, OrbiterTimetable)
                else set()
            )
        )

        imports = [i._to_ast() for i in sorted(pre_imports)]

        # foo = BashOperator(...)
        task_definitions = list(
            dedupe_callable([task._to_ast() for task in self.tasks.values()])
        )

        # foo >> bar
        task_dependencies = sorted(
            [
                downstream
                for task in self.tasks.values()
                for downstream in task.downstream
            ]
        )
        task_dependencies = [downstream._to_ast() for downstream in task_dependencies]

        # with DAG(...) as dag:
        with_dag = py_with(
            self._dag_to_ast().value, body=task_definitions + task_dependencies
        )
        return [
            *imports,
            with_dag,
        ]


# https://github.com/pydantic/pydantic/issues/8790
OrbiterDAG.add_tasks = validate_call()(OrbiterDAG.add_tasks)
