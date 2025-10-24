from __future__ import annotations

from typing import Annotated

from pydantic import Field

from orbiter.objects.timetables.multiple_cron_trigger_timetable import OrbiterMultipleCronTriggerTimetable
from orbiter.objects.timetables.timetable import OrbiterTimetable

TimetableType = Annotated[
    OrbiterTimetable | OrbiterMultipleCronTriggerTimetable,
    Field(discriminator="orbiter_type"),
]
