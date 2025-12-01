from __future__ import annotations

import ast
from typing import Literal

from pydantic import BaseModel

from orbiter.ast_helper import OrbiterASTBase, py_object
from orbiter.objects import ImportList, OrbiterBase, OrbiterRequirement, RenderAttributes

__mermaid__ = """
--8<-- [start:mermaid-dag-relationships]
OrbiterDAG "via schedule" --> OrbiterDataset
--8<-- [end:mermaid-dag-relationships]
"""


class OrbiterDataset(OrbiterBase, OrbiterASTBase, BaseModel, extra="allow"):
    """An [Airflow Dataset](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html)
    reference, typically used for Dataset-based scheduling.

    The primary field is the Dataset ``uri``; any additional keyword arguments
    are passed through to the underlying ``Dataset`` constructor.

    ```pycon
    >>> OrbiterDataset(uri="s3://bucket/key")
    Dataset('s3://bucket/key')
    >>> from orbiter.objects.dag import OrbiterDAG
    >>> OrbiterDAG(
    ...     dag_id="foo",
    ...     file_path="foo.py",
    ...     schedule=OrbiterDataset(uri="db://table")
    ... ) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    from airflow import DAG
    from airflow.datasets import Dataset
    ...
    with DAG(dag_id='foo', schedule=Dataset('db://table')):
    ...
    >>> OrbiterDAG(
    ...     dag_id="foo",
    ...     file_path="foo.py",
    ...     schedule=[
    ...         OrbiterDataset(uri="db://table1"),
    ...         OrbiterDataset(uri="db://table2"),
    ...     ],
    ... ) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
    from airflow import DAG
    from airflow.datasets import Dataset
    ...
    with DAG(dag_id='foo', schedule=[Dataset('db://table1'), Dataset('db://table2')]...

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
        dataset_names = [name for _import in self.imports for name in _import.names if name == "Dataset"]
        if len(dataset_names) != 1:
            raise ValueError(f"Expected exactly one Dataset name, got {dataset_names}")
        [dataset] = dataset_names

        return py_object(
            dataset,
            self.uri,
            # Any additional model fields (from model_extra) should be forwarded
            **{k: getattr(self, k) for k in (self.model_extra.keys() or [])},
        )


# Rebuild the model to resolve forward references in OrbiterBase and other parent classes
OrbiterDataset.model_rebuild()
