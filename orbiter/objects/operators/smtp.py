from __future__ import annotations

from orbiter.objects import OrbiterRequirement, ImportList
from orbiter.objects.task import OrbiterOperator, RenderAttributes

__mermaid__ = """
--8<-- [start:mermaid-relationships]
OrbiterOperator "implements" <|-- OrbiterEmailOperator
--8<-- [end:mermaid-relationships]
"""


class OrbiterEmailOperator(OrbiterOperator):
    # noinspection GrazieInspection
    """
    An Airflow [EmailOperator](https://registry.astronomer.io/providers/apache-airflow/versions/latest/modules/EmailOperator)

    ```pycon
    >>> OrbiterEmailOperator(
    ...   task_id="foo", to="humans@astronomer.io", subject="Hello", html_content="World!"
    ... )
    foo_task = EmailOperator(task_id='foo', to='humans@astronomer.io', subject='Hello', html_content='World!')

    ```
    :param task_id: The task_id for the operator
    :type task_id: str
    :param to: The recipient of the email
    :type to: str | list[str]
    :param subject: The subject of the email
    :type subject: str
    :param html_content: The content of the email
    :type html_content: str
    :param files: The files to attach to the email, defaults to None
    :type files: list | None, optional

    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    operator = "EmailOperator"
    task_id: str
    to: str | list[str]
    subject: str
    html_content: str
    files: list | None
    --8<-- [end:mermaid-props]
    """

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow-providers-smtp",
            module="airflow.providers.smtp.operators.smtp",
            names=["EmailOperator"],
        )
    ]
    operator: str = "EmailOperator"
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes + [
        "to",
        "subject",
        "html_content",
        "files",
    ]
    to: str | list[str]
    subject: str
    html_content: str
    files: list | None = []
