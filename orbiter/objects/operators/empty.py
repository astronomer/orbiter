from typing import Literal

from orbiter.objects import ImportList, RenderAttributes
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.task import OrbiterOperator


class OrbiterEmptyOperator(OrbiterOperator):
    """
    An Airflow EmptyOperator. Does nothing.

    ```pycon
    >>> OrbiterEmptyOperator(task_id="foo")
    foo_task = EmptyOperator(task_id='foo')

    ```
    :param task_id: The `task_id` for the operator
    :type task_id: str
    :param **kwargs: Extra arguments to pass to the operator
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    operator = "EmptyOperator"
    task_id: str
    --8<-- [end:mermaid-props]
    """
    orbiter_type: Literal["OrbiterEmptyOperator"] = "OrbiterEmptyOperator"

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow",
            module="airflow.operators.empty",
            names=["EmptyOperator"],
        )
    ]
    operator: str = "EmptyOperator"
    # noinspection Pydantic
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes
