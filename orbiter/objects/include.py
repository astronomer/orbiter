from pydantic import BaseModel


class OrbiterInclude(BaseModel, extra="forbid"):
    """Represents an included file in an `/include` directory

    :param filepath: The relative path (from the output directory) to write the file to
    :type filepath: str
    :param contents: The contents of the file
    :type contents: str
    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    filepath: str
    contents: str
    --8<-- [end:mermaid-props]
    """

    filepath: str
    contents: str

    def __hash__(self):
        return hash(self.contents + self.filepath)

    def render(self):
        return self.contents
