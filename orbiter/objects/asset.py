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


class OrbiterAsset(OrbiterBase, OrbiterASTBase, extra="allow"):
    """An [Airflow Asset](https://airflow.apache.org/docs/task-sdk/stable/api.html#assets)
    reference, typically used for Asset-based scheduling.

    The primary field is the Asset `uri`; any additional keyword arguments
    are passed through to the underlying `Asset` constructor (e.g., `name`,
    `group`, `extra`, `watchers`).

    ```pycon
    >>> from orbiter.ast_helper import render_ast
    >>> from orbiter.objects.dag import OrbiterDAG
    >>> render_ast(OrbiterAsset(uri="s3://bucket/key")._to_ast())
    "Asset('s3://bucket/key')"
    >>> OrbiterDAG(
    ...     dag_id="foo",
    ...     file_path="foo.py",
    ...     schedule=OrbiterAsset(uri="db://table")
    ... ) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    from airflow import DAG
    ...
    with DAG(dag_id='foo', schedule=Asset('db://table')):
    ...
    >>> OrbiterDAG(
    ...     dag_id="foo",
    ...     file_path="foo.py",
    ...     schedule=[
    ...         OrbiterAsset(uri="db://table1"),
    ...         OrbiterAsset(uri="db://table2"),
    ...     ],
    ... ) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    from airflow import DAG
    ...
    with DAG(dag_id='foo', schedule=[Asset('db://table1'), Asset('db://table2')]...

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


# Rebuild the model to resolve forward references in OrbiterBase and other parent classes
OrbiterAsset.model_rebuild()
