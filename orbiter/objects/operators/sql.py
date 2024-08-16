from orbiter.objects import ImportList, OrbiterRequirement
from orbiter.objects.task import OrbiterOperator, RenderAttributes

__mermaid__ = """
--8<-- [start:mermaid-relationships]
OrbiterOperator "implements" <|-- OrbiterSQLExecuteQueryOperator
--8<-- [end:mermaid-relationships]
"""


class OrbiterSQLExecuteQueryOperator(OrbiterOperator):
    """
    An Airflow
    [Generic SQL Operator](https://registry.astronomer.io/providers/apache-airflow-providers-common-sql/versions/1.10.1/modules/SQLExecuteQueryOperator).
    Used to run SQL against any Database.

    ```pycon
    >>> OrbiterSQLExecuteQueryOperator(
    ...   task_id="foo", conn_id='sql', sql="select 1;"
    ... )
    foo_task = SQLExecuteQueryOperator(task_id='foo', conn_id='sql', sql='select 1;')

    ```
    :param task_id: The `task_id` for the operator
    :type task_id: str
    :param conn_id: The SQL connection to utilize.  (Note: use the `**conn_id(...)` utility function)
    :type conn_id: str
    :param sql: The SQL to execute
    :type sql: str
    :param **kwargs: Extra arguments to pass to the operator
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    operator = "SQLExecuteQueryOperator"
    task_id: str
    conn_id: str
    sql: str
    --8<-- [end:mermaid-props]
    """

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow-providers-common-sql",
            module="airflow.providers.common.sql.operators.sql",
            names=["SQLExecuteQueryOperator"],
        )
    ]
    operator: str = "SQLExecuteQueryOperator"
    conn_id: str
    sql: str

    # noinspection Pydantic
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes + [
        "conn_id",
        "sql",
    ]
