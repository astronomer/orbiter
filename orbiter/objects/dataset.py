from __future__ import annotations

import ast
from typing import Literal

from orbiter.ast_helper import OrbiterASTBase, py_object
from orbiter.objects import ImportList, OrbiterBase, RenderAttributes
from orbiter.objects.requirement import OrbiterRequirement

__mermaid__ = """
--8<-- [start:mermaid-dag-relationships]
OrbiterDAG "via schedule" --> OrbiterDataset
--8<-- [end:mermaid-dag-relationships]
"""


class OrbiterDataset(OrbiterASTBase, OrbiterBase, extra="allow"):
    """An [Airflow Dataset](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html)
    reference, typically used for Dataset-based scheduling.

    The primary field is the Dataset ``uri``; any additional keyword arguments
    are passed through to the underlying ``Dataset`` constructor.

    ```pycon
    >>> OrbiterDataset(uri="s3://bucket/key")
    Dataset('s3://bucket/key')

    ```
    :param uri: The Dataset URI, e.g. ``\"db://table\"`` or ``\"s3://bucket/key\"``
    :type uri: str
    :param **kwargs: any other kwargs to provide to ``Dataset``
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    imports: List[OrbiterRequirement]
    uri: str
    --8<-- [end:mermaid-props]
    """

    orbiter_type: Literal["OrbiterDataset"] = "OrbiterDataset"

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow",
            module="airflow.datasets",
            names=["Dataset"],
        )
    ]

    render_attributes: RenderAttributes = []

    uri: str

    def _to_ast(self) -> ast.stmt | ast.Module:
        return py_object(
            "Dataset",
            self.uri,
            # Any additional model fields (from model_extra) should be forwarded
            **{k: getattr(self, k) for k in (self.model_extra.keys() or [])},
        )
