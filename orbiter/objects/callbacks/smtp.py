from __future__ import annotations

from typing import Iterable, Set

from orbiter.objects.connection import OrbiterConnection

from orbiter.objects import OrbiterRequirement, ImportList
from orbiter.objects.callbacks import OrbiterCallback
from orbiter.objects.task import RenderAttributes


__mermaid__ = """
--8<-- [start:mermaid-relationships]
OrbiterCallback "implements" <|--  OrbiterSmtpNotifierCallback
--8<-- [end:mermaid-relationships]
"""


class OrbiterSmtpNotifierCallback(OrbiterCallback, extra="allow"):
    """
    An [Airflow SMTP Callback (link)](https://airflow.apache.org/docs/apache-airflow-providers-smtp/stable/_api/airflow/providers/smtp/notifications/smtp/index.html)

    Usage notes: use `smtp_conn_id` and reference an SMTP Connection

    ```pycon
    >>> [_import] = OrbiterSmtpNotifierCallback(to="foo@test.com").imports; _import
    OrbiterRequirements(names=[send_smtp_notification], package=apache-airflow-providers-smtp, module=airflow.providers.smtp.notifications.smtp, sys_package=None)

    >>> OrbiterSmtpNotifierCallback(to="foo@test.com", from_email="bar@test.com", subject="Hello", html_content="World")
    send_smtp_notification(to='foo@test.com', from_email='bar@test.com', smtp_conn_id='SMTP', subject='Hello', html_content='World')

    ```

    :param to: The email address to send to
    :type to: str | Iterable[str]
    :param from_email: The email address to send from
    :type from_email: str | None
    :param smtp_conn_id: The connection id to use
    :type smtp_conn_id: str | None
    :param subject: The subject of the email
    :type subject: str | None
    :param html_content: The content of the email
    :type html_content: str | None
    :param cc: The email address to cc
    :type cc: str | Iterable[str] | None
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    to: str
    from_email: str
    subject: str
    html_content: str
    cc: str | Iterable[str]
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

    to: str | Iterable[str]
    from_email: str | None = None
    smtp_conn_id: str | None = "SMTP"
    orbiter_conns: Set[OrbiterConnection] | None = {
        OrbiterConnection(conn_id="SMTP", conn_type="smtp")
    }
    subject: str | None = None
    html_content: str | None = None
    cc: str | Iterable[str] | None = None

    render_attributes: RenderAttributes = [
        "to",
        "from_email",
        "smtp_conn_id",
        "subject",
        "html_content",
        "cc",
    ]
