from __future__ import annotations

from typing import Dict, Literal

from orbiter.objects import ImportList, RenderAttributes
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.task import OrbiterOperator


class OrbiterSSHOperator(OrbiterOperator):
    """
    An Airflow
    [SSHOperator](https://registry.astronomer.io/providers/apache-airflow-providers-ssh/versions/latest/modules/SSHOperator).
    Used to run shell commands over SSH.

    ```pycon
    >>> OrbiterSSHOperator(task_id="foo", ssh_conn_id="SSH", command="echo 'hello world'")
    foo_task = SSHOperator(task_id='foo', ssh_conn_id='SSH', command="echo 'hello world'")

    ```
    :param task_id: The `task_id` for the operator
    :type task_id: str
    :param ssh_conn_id: The SSH connection to use. (Note: use the `**conn_id(...)` utility function)
    :type ssh_conn_id: str
    :param command: The command to execute
    :type command: str
    :param environment: The environment variables to set, defaults to None
    :type environment: dict, optional
    :param **kwargs: Extra arguments to pass to the operator
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    operator = "SSHOperator"
    task_id: str
    ssh_conn_id: str
    command: str
    environment: Dict[str, str] | None
    --8<-- [end:mermaid-props]
    """
    orbiter_type: Literal["OrbiterSSHOperator"] = "OrbiterSSHOperator"

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow-providers-ssh",
            module="airflow.providers.ssh.operators.ssh",
            names=["SSHOperator"],
        )
    ]
    operator: str = "SSHOperator"
    # noinspection Pydantic
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes + [
        "ssh_conn_id",
        "command",
        "environment",
    ]

    ssh_conn_id: str
    command: str
    environment: Dict[str, str] | None = None
