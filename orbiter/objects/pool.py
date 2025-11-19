from typing import ClassVar

from pydantic import BaseModel

from orbiter.objects.airflow_settings import AirflowSettingsRender


class OrbiterPool(BaseModel, AirflowSettingsRender, extra="forbid"):
    """An Airflow
    [Pool](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html),
    rendered to an
    [`airflow_settings.yaml`](https://www.astronomer.io/docs/astro/cli/develop-project#configure-airflow_settingsyaml-local-development-only)
    file.


    ```pycon
    >>> OrbiterPool(name="foo", description="bar", slots=5).render()
    {'pool_name': 'foo', 'pool_description': 'bar', 'pool_slot': 5}

    ```

    !!! note

        Use the utility `pools` function to easily generate both an `OrbiterPool`
        and `pool` property for an operator

        ```python
        from orbiter.objects import pools

        OrbiterTask(
            ...,
            **pools("my_pool"),
        )
        ```

    :param name: The name of the pool
    :type name: str
    :param description: The description of the pool
    :type description: str, optional
    :param slots: The number of slots in the pool. Defaults to 128
    :type slots: int, optional
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    name: str
    description: str | None
    slots: int | None
    --8<-- [end:mermaid-props]
    """

    render_prefix: ClassVar[str] = "pool_"

    name: str
    description: str = ""
    slots: int = 128

    def __add__(self, other) -> "OrbiterPool":
        self.slots = max((self.slots, other.slots))
        self.description = max((self.description, other.description))
        return self

    def __hash__(self):
        return hash(f"{self.model_dump_json}")

    def render(self) -> dict:
        # airflow_settings.yaml takes 'slot' not 'slots'
        res = super().render()
        res[self.render_prefix + "slot"] = res[self.render_prefix + "slots"]
        del res[self.render_prefix + "slots"]
        return res
