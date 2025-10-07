from __future__ import annotations

import ast
from pickle import UnpicklingError  # nosec: B403

import dill  # nosec: B403
from typing import Callable, Literal

from pydantic import field_serializer, field_validator

from orbiter.ast_helper import py_function
from orbiter.objects import ImportList, RenderAttributes
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.task import OrbiterOperator

__mermaid__ = """
--8<-- [start:mermaid-relationships]
OrbiterPythonOperator "implements" <|-- OrbiterDecoratedPythonOperator
--8<-- [end:mermaid-relationships]
"""


class OrbiterPythonOperator(OrbiterOperator):
    """
    An Airflow
    [PythonOperator](https://registry.astronomer.io/providers/apache-airflow/versions/latest/modules/PythonOperator).
    Used to execute any Python Function.

    ```pycon
    >>> def foo(a, b):
    ...    print(a + b)
    >>> OrbiterPythonOperator(task_id="foo", python_callable=foo)
    def foo(a, b):
       print(a + b)
    foo_task = PythonOperator(task_id='foo', python_callable=foo)

    ```

    You can utilize the `orbiter_includes` and `imports` to include additional Python code and imports (or subclass to default).
    ```pycon
    >>> from orbiter.objects.include import OrbiterInclude
    >>> OrbiterPythonOperator(
    ...     task_id="foo",
    ...     orbiter_includes={OrbiterInclude(filepath="include/bar.py", contents="def baz(): pass")},
    ...     imports=[OrbiterRequirement(module="include.bar", names=["baz"])],
    ...     python_callable="baz"
    ... )
    foo_task = PythonOperator(task_id='foo', python_callable=baz)

    ```
    :param task_id: The `task_id` for the operator
    :type task_id: str
    :param python_callable: The python function to execute
    :type python_callable: Callable
    :param op_args: The arguments to pass to the python function, defaults to None
    :type op_args: list | None, optional
    :param op_kwargs: The keyword arguments to pass to the python function, defaults to None
    :type op_kwargs: dict | None, optional
    :param **kwargs: Extra arguments to pass to the operator
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    operator = "PythonOperator"
    task_id: str
    python_callable: Callable
    op_args: list | None
    op_kwargs: dict | None
    --8<-- [end:mermaid-props]
    """
    orbiter_type: Literal["OrbiterPythonOperator"] = "OrbiterPythonOperator"

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow",
            module="airflow.operators.python",
            names=["PythonOperator"],
        )
    ]
    operator: str = "PythonOperator"
    # noinspection Pydantic
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes + [
        "python_callable",
        "op_args",
        "op_kwargs",
    ]

    python_callable: Callable | str

    @field_serializer("python_callable", when_used="json-unless-none")
    def serialize_python_callable(self, v) -> str:
        import codecs

        return codecs.encode(dill.dumps(v), "base64").decode()

    @field_validator("python_callable", mode="before")
    @classmethod
    def validate_python_callable(cls, v: str | Callable) -> Callable | str:
        import codecs

        if isinstance(v, bytes):
            return dill.loads(codecs.decode(v, "base64"))  # nosec: B301
        elif isinstance(v, str):
            try:
                return dill.loads(codecs.decode(v.encode(), "base64"))  # nosec: B301
            except (UnpicklingError, ValueError):
                return v
        elif isinstance(v, Callable):
            return v
        raise ValueError("python_callable must be a dill-compatible bytes as a string or callable")

    op_args: list | None = None
    op_kwargs: dict | None = None

    def _to_ast(self):
        if isinstance(self.python_callable, Callable):
            return [py_function(self.python_callable), super()._to_ast()]
        return super()._to_ast()


class OrbiterDecoratedPythonOperator(OrbiterPythonOperator):
    """
    An Airflow
    [TaskFlow @task](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html).
    Used to execute any Python Function.

    ```pycon
    >>> def foo(a, b):
    ...    print(a + b)
    >>> OrbiterDecoratedPythonOperator(task_id="foo", python_callable=foo)
    @task()
    def foo(a, b):
        print(a + b)

    ```
    Parameters as per [`OrbiterPythonOperator`][orbiter.objects.operators.python.OrbiterPythonOperator]
    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    operator = "@task"
    task_id: str
    --8<-- [end:mermaid-props]
    """
    orbiter_type: Literal["OrbiterDecoratedPythonOperator"] = "OrbiterDecoratedPythonOperator"

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow",
            module="airflow.decorators",
            names=["task"],
        )
    ]

    def _to_ast(self) -> ast.stmt:
        return py_function(
            self.python_callable,
            decorator_names=["task"],
            decorator_kwargs=[{}],
        )
