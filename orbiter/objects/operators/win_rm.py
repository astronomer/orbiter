from __future__ import annotations

from typing import Literal

from orbiter.objects.requirement import OrbiterRequirement

from orbiter.objects import ImportList, RenderAttributes
from orbiter.objects.task import OrbiterOperator


class OrbiterWinRMOperator(OrbiterOperator):
    """
    An Airflow
    [WinRMOperator](https://registry.astronomer.io/providers/apache-airflow-providers-microsoft-winrm/versions/latest/modules/WinRMOperator).
    Used to run commands on remote Window Servers.

    ```pycon
    >>> from orbiter.objects import conn_id
    >>> OrbiterWinRMOperator(task_id="foo", **conn_id(conn_id="winrm_default", prefix="ssh", conn_type="ssh"), command="echo hello")
    foo_task = WinRMOperator(task_id='foo', ssh_conn_id='winrm_default', command='echo hello')

    ```
    :param task_id: The `task_id` for the operator
    :type task_id: str
    :param ssh_conn_id: The SSH connection to use. (Note: use the `**conn_id(...)` utility function)
    :type ssh_conn_id: str
    :param command: The command to execute
    :type command: str
    :param **kwargs: Extra arguments to pass to the operator
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """  # noqa: E501

    __mermaid__ = """
     --8<-- [start:mermaid-props]
     operator = "WinRMOperator"
     task_id: str
     ssh_conn_id: str
     command: str
     kwargs: Any
     --8<-- [end:mermaid-props]
     """
    orbiter_type: Literal["OrbiterWinRMOperator"] = "OrbiterWinRMOperator"

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow-providers-microsoft-winrm",
            module="airflow.providers.microsoft.winrm.operators.winrm",
            names=["WinRMOperator"],
        )
    ]
    operator: str = "WinRMOperator"
    # noinspection Pydantic
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes + [
        "ssh_conn_id",
        "command",
    ]
    ssh_conn_id: str
    command: str
