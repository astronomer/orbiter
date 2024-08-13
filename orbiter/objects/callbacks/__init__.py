import ast
from abc import ABC

from orbiter.ast_helper import OrbiterASTBase, py_object
from orbiter.objects import OrbiterBase, ImportList, OrbiterRequirement
from orbiter.objects.task import RenderAttributes


__mermaid__ = """
--8<-- [start:mermaid-relationships]
--8<-- [end:mermaid-relationships]
"""


class OrbiterCallback(OrbiterASTBase, OrbiterBase, ABC, extra="forbid"):
    """An Airflow
    [callback function](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html),
    which might be used in `DAG.on_failure_callback`, or `Task.on_success_callback`, or etc.

    `OrbiterCallback` is an Abstract Base Class, and should be subclassed to create a new callback.

    ```pycon
    >>> class OrbiterMyCallback(OrbiterCallback):
    ...   function: str = "my_callback"
    ...   foo: str
    ...   bar: str
    ...   render_attributes: RenderAttributes = ["foo", "bar"]
    >>> OrbiterMyCallback(foo="fop", bar="bop")
    my_callback(foo='fop', bar='bop')

    ```

    :param function: The name of the function to call
    :type function: str
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    function: str
    --8<-- [end:mermaid-props]
    """

    imports: ImportList = [OrbiterRequirement(package="apache-airflow")]
    function: str
    render_attributes: RenderAttributes = []

    def _to_ast(self) -> ast.Expr:
        return py_object(
            name=self.function,
            **{
                k: getattr(self, k)
                for k in self.render_attributes
                if (k and getattr(self, k)) or (k == "from_email")
            },
        )
