from pydantic import BaseModel


class OrbiterEnvVar(BaseModel, extra="forbid"):
    """Represents an Environmental Variable, renders to a line in `.env` file

    ```pycon
    >>> OrbiterEnvVar(key="foo", value="bar").render()
    'foo=bar'

    ```

    :param key: The key of the environment variable
    :type key: str
    :param value: The value of the environment variable
    :type value: str
    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    key: str
    value: str
    --8<-- [end:mermaid-props]
    """

    key: str
    value: str

    def __hash__(self):
        return hash(f"{self.model_dump_json()}")

    def render(self) -> str:
        return f"{self.key}={self.value}"
