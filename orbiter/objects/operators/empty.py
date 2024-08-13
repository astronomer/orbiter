from orbiter.objects import ImportList
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.task import OrbiterOperator, RenderAttributes

__mermaid__ = """
--8<-- [start:mermaid-relationships]
OrbiterOperator  "implements" <|--  OrbiterEmptyOperator
--8<-- [end:mermaid-relationships]
"""


class OrbiterEmptyOperator(OrbiterOperator):
    """
    An Airflow EmptyOperator

    ```pycon
    >>> OrbiterEmptyOperator(task_id="foo")
    foo_task = EmptyOperator(task_id='foo')

    ```
    :param task_id: The task_id for the operator
    :type task_id: str
    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    operator = "BashOperator"
    task_id: str
    --8<-- [end:mermaid-props]
    """

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow",
            module="airflow.operators.empty",
            names=["EmptyOperator"],
        )
    ]
    operator: str = "EmptyOperator"
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes
