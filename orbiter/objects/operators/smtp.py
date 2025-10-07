from __future__ import annotations

from typing import Set, Literal

from orbiter.objects.requirement import OrbiterRequirement

from orbiter.objects.connection import OrbiterConnection

from orbiter.objects import ImportList, RenderAttributes
from orbiter.objects.task import OrbiterOperator


class OrbiterEmailOperator(OrbiterOperator):
    # noinspection GrazieInspection
    """
    An Airflow
    [EmailOperator](https://registry.astronomer.io/providers/apache-airflow/versions/latest/modules/EmailOperator).
    Used to send emails.

    ```pycon
    >>> OrbiterEmailOperator(
    ...   task_id="foo", to="humans@astronomer.io", subject="Hello", html_content="World!"
    ... )
    foo_task = EmailOperator(task_id='foo', to='humans@astronomer.io', subject='Hello', html_content='World!', conn_id='SMTP')

    ```
    :param task_id: The `task_id` for the operator
    :type task_id: str
    :param to: The recipient of the email
    :type to: str | list[str]
    :param subject: The subject of the email
    :type subject: str
    :param html_content: The content of the email
    :type html_content: str
    :param files: The files to attach to the email, defaults to None
    :type files: list, optional
    :param conn_id: The SMTP connection to use. Defaults to "SMTP" and sets `orbiter_conns` property
    :type conn_id: str, optional
    :param **kwargs: Extra arguments to pass to the operator
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    operator = "EmailOperator"
    task_id: str
    to: str | list[str]
    subject: str
    html_content: str
    files: list | None
    conn_id: str
    --8<-- [end:mermaid-props]
    """
    orbiter_type: Literal["OrbiterEmailOperator"] = "OrbiterEmailOperator"

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow-providers-smtp",
            module="airflow.providers.smtp.operators.smtp",
            names=["EmailOperator"],
        )
    ]
    operator: str = "EmailOperator"
    # noinspection Pydantic
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes + [
        "to",
        "subject",
        "html_content",
        "files",
        "conn_id",
    ]
    to: str | list[str]
    subject: str
    html_content: str
    files: list | None = []
    conn_id: str | None = "SMTP"
    orbiter_conns: Set[OrbiterConnection] | None = {OrbiterConnection(conn_id="SMTP", conn_type="smtp")}
