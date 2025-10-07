from typing import Literal

from orbiter.objects import ImportList, RenderAttributes
from orbiter.objects.requirement import OrbiterRequirement
from orbiter.objects.task import OrbiterOperator


class OrbiterBashOperator(OrbiterOperator):
    """
    An Airflow
    [BashOperator](https://registry.astronomer.io/providers/apache-airflow/versions/latest/modules/BashOperator).
    Used to run shell commands.

    ```pycon
    >>> OrbiterBashOperator(task_id="foo", bash_command="echo 'hello world'")
    foo_task = BashOperator(task_id='foo', bash_command="echo 'hello world'")

    ```
    :param task_id: The `task_id` for the operator
    :type task_id: str
    :param bash_command: The shell command to execute
    :type bash_command: str
    :param **kwargs: Extra arguments to pass to the operator
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    operator = "BashOperator"
    task_id: str
    bash_command: str
    --8<-- [end:mermaid-props]
    """

    orbiter_type: Literal["OrbiterBashOperator"] = "OrbiterBashOperator"
    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow",
            module="airflow.operators.bash",
            names=["BashOperator"],
        )
    ]
    operator: str = "BashOperator"
    # noinspection Pydantic
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes + ["bash_command"]

    bash_command: str
