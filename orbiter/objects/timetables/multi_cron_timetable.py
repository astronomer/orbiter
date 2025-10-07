from __future__ import annotations

from importlib.util import find_spec
from pathlib import Path
from typing import List, Set

from orbiter.objects import OrbiterRequirement, ImportList
from orbiter.objects.include import OrbiterInclude
from orbiter.objects import RenderAttributes
from orbiter.objects.timetables import OrbiterTimetable


__mermaid__ = """
--8<-- [start:mermaid-dag-relationships]
OrbiterTimetable "implements" <|-- OrbiterMultiCronTimetable
--8<-- [end:mermaid-dag-relationships]
"""


class OrbiterMultiCronTimetable(OrbiterTimetable):
    """
    An
    [Airflow Timetable](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html)
    that can be supplied with multiple cron strings.

    ```pycon
    >>> OrbiterMultiCronTimetable(cron_defs=["*/5 * * * *", "*/7 * * * *"])
    MultiCronTimetable(cron_defs=['*/5 * * * *', '*/7 * * * *'])

    ```
    :params cron_defs: A list of cron strings
    :type cron_defs: List[str]
    :params timezone: The timezone to use for the timetable
    :type timezone: str
    :params period_length: The length of the period
    :type period_length: int
    :params period_unit: The unit of the period
    :type period_unit: str
    """

    __mermaid__ = """
    --8<-- [start:mermaid-props]
    timetable = "MultiCronTimetable"
    cron_defs: List[str]
    timezone: str
    period_length: int
    period_unit: str
    --8<-- [end:mermaid-props]
    """

    imports: ImportList = [
        OrbiterRequirement(
            package="croniter",
            module="include.multi_cron_timetable",
            names=["MultiCronTimetable"],
        )
    ]
    orbiter_includes: Set["OrbiterInclude"] = {
        OrbiterInclude(
            filepath="include/multi_cron_timetable.py",
            contents=Path(
                find_spec("orbiter.assets.timetables.multi_cron.multi_cron_timetable_src").origin
            ).read_text(),
        ),
        OrbiterInclude(
            filepath="plugins/multi_cron_timetable.py",
            contents=Path(
                find_spec("orbiter.assets.timetables.multi_cron.multi_cron_timetable_plugin_src").origin
            ).read_text(),
        ),
    }
    render_attributes: RenderAttributes = [
        "cron_defs",
        "timezone",
        "period_length",
        "period_unit",
    ]

    cron_defs: List[str]
    timezone: str | None = None
    period_length: int | None = None
    period_unit: str | None = None
