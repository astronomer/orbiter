from orbiter.objects import ImportList, OrbiterRequirement
from orbiter.objects.task import OrbiterOperator, RenderAttributes


class OrbiterLivyOperator(OrbiterOperator):
    """
    An Airflow
    [LivyOperator](https://registry.astronomer.io/providers/apache-airflow-providers-apache-livy/versions/3.9.1/modules/LivyOperator).
    Used to submit Spark Jobs.

    ```pycon
    >>> OrbiterLivyOperator(task_id="foo", file="/a/b/c.jar")
    foo_task = LivyOperator(task_id='foo', file='/a/b/c.jar')

    ```
    :param task_id: The `task_id` for the operator
    :type task_id: str
    :param **kwargs: Extra arguments to pass to the operator
    :param **OrbiterBase: [OrbiterBase][orbiter.objects.OrbiterBase] inherited properties
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    operator = "EmailOperator"
    task_id: str
    kwargs: Any
    --8<-- [end:mermaid-props]
    """

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow-providers-apache-livy",
            module="airflow.providers.apache.livy.operators.livy",
            names=["LivyOperator"],
        )
    ]
    operator: str = "LivyOperator"

    # noinspection Pydantic
    render_attributes: RenderAttributes = OrbiterOperator.render_attributes


if __name__ == "__main__":
    import doctest

    doctest.testmod()
