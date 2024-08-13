from typing import ClassVar

from pydantic import BaseModel

from orbiter.objects.airflow_settings import AirflowSettingsRender


class OrbiterVariable(BaseModel, AirflowSettingsRender, extra="forbid"):
    """An Airflow
    [Variable](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html),
    rendered to an
    [`airflow_settings.yaml`](https://www.astronomer.io/docs/astro/cli/develop-project#configure-airflow_settingsyaml-local-development-only)
    file.

    ```pycon
    >>> OrbiterVariable(key="foo", value="bar").render()
    {'variable_value': 'bar', 'variable_name': 'foo'}

    ```
    :param key: The key of the variable
    :type key: str
    :param value: The value of the variable
    :type value: str
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    key: str
    value: str
    --8<-- [end:mermaid-props]
    """

    render_prefix: ClassVar[str] = "variable_"

    key: str
    value: str

    def __hash__(self):
        return hash(f"{self.model_dump_json()}")

    def render(self) -> dict:
        # airflow_settings.yaml takes 'name' not 'key'
        res = super().render()
        res[self.render_prefix + "name"] = res[self.render_prefix + "key"]
        del res[self.render_prefix + "key"]
        return res
