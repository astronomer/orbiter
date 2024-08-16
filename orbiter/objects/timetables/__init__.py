from __future__ import annotations

import ast

from pydantic import BaseModel

from orbiter.ast_helper import OrbiterASTBase, py_object
from orbiter.objects import OrbiterBase
from orbiter.objects.task import RenderAttributes


class OrbiterTimetable(OrbiterBase, OrbiterASTBase, BaseModel, extra="allow"):
    """An [Airflow Timetable](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html)
    reference.

    Utilizes [`OrbiterInclude`][orbiter.objects.include.OrbiterInclude]
    to add a file to a /plugins folder to register the timetable.

    :param **kwargs: any other kwargs to provide to Timetable
    """  # noqa: E501

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    imports: List[OrbiterRequirements]
    orbiter_includes: Set[OrbiterIncludes]
    **kwargs: dict
    --8<-- [end:mermaid-props]
    """

    render_attributes: RenderAttributes = []

    def _to_ast(self) -> ast.stmt | ast.Module:
        # Figure out which timetable we are talking about
        timetable_names = [
            name
            for _import in self.imports
            for name in _import.names
            if "timetable" in name.lower()
        ]
        if len(timetable_names) != 1:
            raise ValueError(
                f"Expected exactly one Timetable name, got {timetable_names}"
            )
        [timetable] = timetable_names

        return py_object(
            timetable,
            **{
                k: getattr(self, k)
                for k in self.render_attributes
                if k and getattr(self, k)
            },
            **{k: getattr(self, k) for k in (self.model_extra.keys() or [])},
        )
