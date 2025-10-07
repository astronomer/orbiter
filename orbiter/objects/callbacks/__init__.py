import ast

from orbiter.ast_helper import OrbiterASTBase, py_object, py_reference
from orbiter.objects import OrbiterBase, ImportList, RenderAttributes
from orbiter.objects.requirement import OrbiterRequirement

__mermaid__ = """
--8<-- [start:mermaid-relationships]
--8<-- [end:mermaid-relationships]
"""


class OrbiterCallback(OrbiterASTBase, OrbiterBase, extra="forbid"):
    """Represents an Airflow
    [callback function](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html),
    which might be used in `DAG.on_failure_callback`, or `Task.on_success_callback`, or etc.

    Can be instantiated directly as a bare callback function (with no arguments):
    ```pycon
    >>> from orbiter.objects.dag import OrbiterDAG
    >>> from orbiter.objects.include import OrbiterInclude
    >>> my_callback = OrbiterCallback(
    ...     function="my_callback",
    ...     imports=[OrbiterRequirement(module="my_callback", names=["my_callback"])],
    ...     orbiter_includes={OrbiterInclude(filepath="my_callback.py", contents="...")}
    ... )
    >>> OrbiterDAG(dag_id='', file_path='', on_failure_callback=my_callback)
    ... # doctest: +ELLIPSIS
    from airflow import DAG
    from my_callback import my_callback
    with DAG(dag_id='', on_failure_callback=my_callback):

    ```

    or be subclassed:
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
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
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
        if self.render_attributes:
            return py_object(
                name=self.function,
                **{
                    k: getattr(self, k)
                    for k in self.render_attributes
                    if (k and getattr(self, k)) or (k == "from_email")
                },
            )
        else:
            return py_reference(self.function)
