from __future__ import annotations

from typing import Set, Sequence

from orbiter.objects.connection import OrbiterConnection

from orbiter.objects import OrbiterRequirement, ImportList, RenderAttributes
from orbiter.objects.callbacks import OrbiterCallback


__mermaid__ = """
--8<-- [start:mermaid-relationships]
OrbiterCallback "implements" <|--  OrbiterSmtpNotifierCallback
--8<-- [end:mermaid-relationships]
"""


class OrbiterSmtpNotifierCallback(OrbiterCallback, extra="allow"):
    """
    An [Airflow SMTP Callback](https://airflow.apache.org/docs/apache-airflow-providers-smtp/stable/_api/airflow/providers/smtp/notifications/smtp/index.html)

    !!! note

        Use `smtp_conn_id` and reference an SMTP Connection.

        You can use the `**conn_id("SMTP", conn_type="smtp")` utility function to set both properties at once.

    ```pycon
    >>> [_import] = OrbiterSmtpNotifierCallback(to="foo@test.com").imports; _import
    OrbiterRequirement(names=[send_smtp_notification], package=apache-airflow-providers-smtp, module=airflow.providers.smtp.notifications.smtp, sys_package=None)
    >>> OrbiterSmtpNotifierCallback(to="foo@test.com", from_email="bar@test.com", subject="Hello", html_content="World")
    send_smtp_notification(to='foo@test.com', from_email='bar@test.com', smtp_conn_id='SMTP', subject='Hello', html_content='World')
    >>> OrbiterSmtpNotifierCallback(to=["foo@test.com", "baz@test.com"], from_email="bar@test.com", subject="Hello", html_content="World")
    send_smtp_notification(to=['foo@test.com', 'baz@test.com'], from_email='bar@test.com', smtp_conn_id='SMTP', subject='Hello', html_content='World')

    ```

    :param to: The email address to send to
    :type to: str | Sequence[str]
    :param from_email: The email address to send from
    :type from_email: str, optional
    :param smtp_conn_id: The connection id to use (Note: use the `**conn_id(...)` utility function). Defaults to "SMTP"
    :type smtp_conn_id: str, optional
    :param subject: The subject of the email
    :type subject: str, optional
    :param html_content: The content of the email
    :type html_content: str, optional
    :param cc: The email address to cc
    :type cc: str | Sequence[str], optional
    :param **kwargs
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    to: str
    from_email: str
    smtp_conn_id: str
    subject: str
    html_content: str
    cc: str | Sequence[str]
    --8<-- [end:mermaid-props]
    """

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow-providers-smtp",
            module="airflow.providers.smtp.notifications.smtp",
            names=["send_smtp_notification"],
        )
    ]
    function: str = "send_smtp_notification"

    to: str | Sequence[str]
    from_email: str | None = None
    smtp_conn_id: str | None = "SMTP"
    orbiter_conns: Set[OrbiterConnection] | None = {OrbiterConnection(conn_id="SMTP", conn_type="smtp")}
    subject: str | None = None
    html_content: str | None = None
    cc: str | Sequence[str] | None = None

    render_attributes: RenderAttributes = [
        "to",
        "from_email",
        "smtp_conn_id",
        "subject",
        "html_content",
        "cc",
    ]
