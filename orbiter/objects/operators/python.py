from __future__ import annotations
from typing import Callable

from orbiter.ast_helper import py_function
from orbiter.objects import ImportList
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.task import OrbiterOperator, RenderAttributes

__mermaid__ = """
--8<-- [start:mermaid-relationships]
OrbiterOperator "implements" <|-- OrbiterPythonOperator
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
    op_args: list | None = None
    op_kwargs: dict | None = None

    def _to_ast(self):
        if isinstance(self.python_callable, Callable):
            return [py_function(self.python_callable), super()._to_ast()]
        return super()._to_ast()


if __name__ == "__main__":
    import doctest

    doctest.testmod(optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.IGNORE_EXCEPTION_DETAIL)
