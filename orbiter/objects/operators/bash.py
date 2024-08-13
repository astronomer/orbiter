from orbiter.objects import ImportList
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.task import OrbiterOperator, RenderAttributes

__mermaid__ = """
--8<-- [start:mermaid-relationships]
OrbiterOperator "implements" <|-- OrbiterBashOperator
--8<-- [end:mermaid-relationships]
"""


class OrbiterBashOperator(OrbiterOperator):
    """
    An Airflow [BashOperator](https://registry.astronomer.io/providers/apache-airflow/versions/latest/modules/BashOperator)

    ```pycon
    >>> OrbiterBashOperator(task_id="foo", bash_command="echo 'hello world'")
    foo_task = BashOperator(task_id='foo', bash_command="echo 'hello world'")

    ```
    :param task_id: The task_id for the operator
    :type task_id: str
    :param bash_command: The bash command to execute
    :type bash_command: str
    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    operator = "BashOperator"
    task_id: str
    bash_command: str
    --8<-- [end:mermaid-props]
    """

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow",
            module="airflow.operators.bash",
            names=["BashOperator"],
        )
    ]
    operator: str = "BashOperator"
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes + [
        "bash_command"
    ]

    bash_command: str
