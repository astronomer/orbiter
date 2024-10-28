from __future__ import annotations

from typing import ClassVar

from pydantic import BaseModel

from orbiter.objects.airflow_settings import AirflowSettingsRender


class OrbiterConnection(BaseModel, AirflowSettingsRender, extra="allow"):
    """An Airflow
    [Connection](https://airflow.apache.org/docs/apache-airflow-providers/core-extensions/connections.html),
    rendered to an
    [`airflow_settings.yaml`](https://www.astronomer.io/docs/astro/cli/develop-project#configure-airflow_settingsyaml-local-development-only)
    file.

    See also other
    [Connection](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/connections.html)
    documentation.

    ```pycon
    >>> OrbiterConnection(
    ...     conn_id="my_conn_id", conn_type="mysql", host="localhost", port=3306, login="root"
    ... ).render()
    {'conn_id': 'my_conn_id', 'conn_type': 'mysql', 'conn_host': 'localhost', 'conn_port': 3306, 'conn_login': 'root'}

    ```

    !!! note

        Use the utility `conn_id` function to generate both an `OrbiterConnection`
        and connection property for an operator

        ```python
        from orbiter.objects import conn_id

        OrbiterTask(
            ...,
            **conn_id("my_conn_id", conn_type="mysql"),
        )
        ```

    :param conn_id: The ID of the connection
    :type conn_id: str
    :param conn_type: The type of the connection, always lowercase. Defaults to 'generic'
    :type conn_type: str, optional
    :param **kwargs: Additional properties for the connection
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    conn_id: str
    conn_type: str
    **kwargs
    --8<-- [end:mermaid-props]
    """
    render_prefix: ClassVar[str] = "conn_"

    conn_id: str
    conn_type: str = "generic"

    def __hash__(self):
        return hash(f"{self.model_dump_json()}")

    def __repr__(self):
        return "OrbiterConnection(" + ", ".join(f"{k}={v}" for k, v in self.model_dump().items()) + ")"
