from __future__ import annotations

from typing import Literal

from orbiter.objects import ImportList, RenderAttributes
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.task import OrbiterOperator


class OrbiterLivyOperator(OrbiterOperator):
    """
    An Airflow
    [LivyOperator](https://registry.astronomer.io/providers/apache-airflow-providers-apache-livy/versions/latest/modules/LivyOperator).
    Used to submit Spark Jobs.

    ```pycon
    >>> from orbiter.objects import conn_id
    >>> OrbiterLivyOperator(task_id="foo", **conn_id(conn_id='livy_default', prefix="livy", conn_type='livy'), file="/a/b/c.jar")
    foo_task = LivyOperator(task_id='foo', livy_conn_id='livy_default', file='/a/b/c.jar')

    ```
    :param task_id: The `task_id` for the operator
    :type task_id: str
    :param livy_conn_id: The Livy connection to use. (Note: use the `**conn_id(...)` utility function)
    :type livy_conn_id: str
    :param **kwargs: Extra arguments to pass to the operator
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    operator = "LivyOperator"
    task_id: str
    livy_conn_id: str
    kwargs: Any
    --8<-- [end:mermaid-props]
    """
    orbiter_type: Literal["OrbiterLivyOperator"] = "OrbiterLivyOperator"

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow-providers-apache-livy",
            module="airflow.providers.apache.livy.operators.livy",
            names=["LivyOperator"],
        )
    ]
    operator: str = "LivyOperator"
    livy_conn_id: str

    # noinspection Pydantic
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes + ["livy_conn_id"]
