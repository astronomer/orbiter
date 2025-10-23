from __future__ import annotations

import ast
from datetime import timedelta
from typing import List, Literal

from orbiter.ast_helper import py_object
from orbiter.objects import OrbiterRequirement, ImportList
from orbiter.objects import RenderAttributes
from orbiter.objects.timetables.timetable import OrbiterTimetable


__mermaid__ = """
--8<-- [start:mermaid-dag-relationships]
OrbiterTimetable "implements" <|-- OrbiterMultipleCronTriggerTimetable
--8<-- [end:mermaid-dag-relationships]
"""


class OrbiterMultipleCronTriggerTimetable(OrbiterTimetable):
    """
    An
    [Airflow Multiple Cron Timetable](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html#multiplecrontriggertimetable)
    that can be supplied with multiple cron strings.

    ```pycon
    >>> OrbiterMultipleCronTriggerTimetable(crons=["*/5 * * * *", "*/7 * * * *"])
    MultipleCronTriggerTimetable('*/5 * * * *', '*/7 * * * *', timezone='UTC')

    ```
    :params crons: A list of cron strings
    :type crons: List[str]
    :params timezone: The timezone to use for the timetable
    :type timezone: str
    :params interval: to set the Dag data interval
    :type interval: timedelta
    :params run_immediately: Whether to run immediately on Dag creation
    :type run_immediately: bool
    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    timetable = "MultipleCronTriggerTimetable"
    crons: List[str]
    timezone: str
    interval: timedelta
    run_immediately: bool
    --8<-- [end:mermaid-props]
    """
    orbiter_type: Literal["OrbiterMultipleCronTriggerTimetable"] = "OrbiterMultipleCronTriggerTimetable"

    imports: ImportList = [
        OrbiterRequirement(
            package="apache-airflow",
            module="airflow.timetables.trigger",
            names=["MultipleCronTriggerTimetable"],
        )
    ]
    render_attributes: RenderAttributes = [
        "timezone",
        "interval",
        "run_immediately",
    ]

    crons: List[str]
    timezone: str = "UTC"
    interval: timedelta | None = None
    run_immediately: bool | None = None

    # noinspection DuplicatedCode
    def _to_ast(self) -> ast.stmt | ast.Module:
        timetable_names = [name for _import in self.imports for name in _import.names if "timetable" in name.lower()]
        if len(timetable_names) != 1:
            raise ValueError(f"Expected exactly one Timetable name, got {timetable_names}")
        [timetable] = timetable_names

        return py_object(
            timetable,
            *self.crons,
            **{k: getattr(self, k) for k in self.render_attributes if k and getattr(self, k)},
            **{k: getattr(self, k) for k in (self.model_extra.keys() or [])},
        )
