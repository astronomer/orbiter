from __future__ import annotations

import ast
from typing import Literal

from orbiter.ast_helper import OrbiterASTBase, py_object
from orbiter.objects import ImportList, OrbiterBase, RenderAttributes
from orbiter.objects.requirement import OrbiterRequirement

__mermaid__ = """
--8<-- [start:mermaid-dag-relationships]
OrbiterDAG "via schedule" --> OrbiterAsset
--8<-- [end:mermaid-dag-relationships]
"""


class OrbiterAsset(OrbiterASTBase, OrbiterBase, extra="allow"):
    """An [Airflow Asset](https://airflow.apache.org/docs/task-sdk/stable/api.html#assets)
    reference, typically used for Asset-based scheduling.

    The primary field is the Asset `uri`; any additional keyword arguments
    are passed through to the underlying `Asset` constructor (e.g., `name`,
    `group`, `extra`, `watchers`).

    ```pycon
    >>> OrbiterAsset(uri="s3://bucket/key")
    Asset('s3://bucket/key')

    ```
    :param uri: The Asset URI, e.g. `db://table` or `s3://bucket/key`
    :type uri: str
    :param **kwargs: any other kwargs to provide to `Asset`
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    imports: List[OrbiterRequirement]
    uri: str
    --8<-- [end:mermaid-props]
    """

    orbiter_type: Literal["OrbiterAsset"] = "OrbiterAsset"

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow",
            module="airflow.sdk.definitions.asset",
            names=["Asset"],
        )
    ]

    render_attributes: RenderAttributes = []

    uri: str

    def _to_ast(self) -> ast.stmt | ast.Module:
        return py_object(
            "Asset",
            self.uri,
            **{k: getattr(self, k) for k in self.model_extra.keys() or []},
        )
