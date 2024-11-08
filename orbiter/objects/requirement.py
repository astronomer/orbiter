from __future__ import annotations

import ast
from typing import List

from pydantic import BaseModel

from orbiter.ast_helper import py_import, OrbiterASTBase


class OrbiterRequirement(OrbiterASTBase, BaseModel, extra="forbid"):
    """A requirement for a project (e.g. `apache-airflow-providers-google`), and it's representation in the DAG file.

    Renders via the DAG File (as an import statement),
    [`requirements.txt`, and `packages.txt`](https://www.astronomer.io/docs/astro/cli/develop-project#add-python-os-level-packages-and-airflow-providers)

    !!! tip

        If a given requirement has multiple packages required,
        it can be defined as multiple `OrbiterRequirement` objects.

        Example:
        ```python
        OrbiterTask(
            ...,
            imports=[
                OrbiterRequirement(package="apache-airflow-providers-google", ...),
                OrbiterRequirement(package="bigquery", sys_package="mysql", ...),
            ],
        )
        ```

    :param package: e.g. `"apache-airflow-providers-google"`
    :type package: str, optional
    :param module: e.g. `"airflow.providers.google.cloud.operators.bigquery"`, defaults to `None`
    :type module: str, optional
    :param names: e.g. `["BigQueryCreateEmptyDatasetOperator"]`, defaults to `[]`
    :type names: List[str], optional
    :param sys_package: e.g. `"mysql"` - represents a **Debian** system package
    :type sys_package: Set[str], optional
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    package: str | None
    module: str | None
    names: List[str] | None
    sys_package: str | None
    --8<-- [end:mermaid-props]
    """

    package: str | None = None
    module: str | None = None
    names: List[str] | None = []
    sys_package: str | None = None

    def __repr__(self):
        return (
            f"OrbiterRequirement("
            f"names=[{','.join(sorted(self.names))}], "
            f"package={self.package}, "
            f"module={self.module}, "
            f"sys_package={self.sys_package})"
        )

    def __hash__(self):
        return hash(f"{self.package}{self.module}{''.join(self.names)}{self.sys_package}")

    def _to_ast(self) -> ast.stmt | ast.Module:
        """Render to `from ... import`
        ```pycon
        >>> from orbiter.ast_helper import render_ast
        >>> render_ast(OrbiterRequirement(
        ...     package="apache-airflow", module="airflow.operators.bash", names=["BashOperator"]
        ... )._to_ast())
        'from airflow.operators.bash import BashOperator'

        ```
        """
        return py_import(module=self.module, names=sorted(self.names))
