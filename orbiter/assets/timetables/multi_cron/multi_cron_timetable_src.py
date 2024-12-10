from typing import Any, Dict, List, Optional

from airflow.exceptions import AirflowTimetableInvalid
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from croniter import croniter
from pendulum import DateTime, Duration, timezone, now
from pendulum import instance as pendulum_instance


class MultiCronTimetable(Timetable):
    valid_units = ["minutes", "hours", "days"]

    def __init__(
        self,
        cron_defs: List[str],
        timezone: str = "UTC",
        period_length: int = 0,
        period_unit: str = "hours",
    ):
        self.cron_defs = cron_defs
        self.timezone = timezone
        self.period_length = period_length
        self.period_unit = period_unit

    def __repr__(self) -> str:
        return "MultiCronTimetable({})".format(self.cron_defs)

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        """
        Determines date interval for manually triggered runs.
        This is simply (now - period) to now.
        """
        end = run_after
        start = end if (self.period_length == 0) else self.data_period_start(end)

        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        """Determines when the DAG should be scheduled."""

        if restriction.earliest is None:
            # No start_date. Don't schedule.
            return None

        is_first_run = last_automated_data_interval is None

        if is_first_run:
            if restriction.catchup:
                scheduled_time = self.next_scheduled_run_time(restriction.earliest)

            else:
                scheduled_time = self.previous_scheduled_run_time()
                if scheduled_time is None:
                    # No previous cron time matched. Find one in the future.
                    scheduled_time = self.next_scheduled_run_time()
        else:
            last_scheduled_time = last_automated_data_interval.end

            if restriction.catchup:
                scheduled_time = self.next_scheduled_run_time(last_scheduled_time)

            else:
                scheduled_time = self.previous_scheduled_run_time()

                if scheduled_time is None or scheduled_time == last_scheduled_time:
                    # No previous cron time matched,
                    # or the matched cron time was the last execution time,
                    scheduled_time = self.next_scheduled_run_time()

                elif scheduled_time > last_scheduled_time:
                    # Matched cron time was after last execution time, but before now.
                    # Use this cron time
                    pass

                else:
                    # The last execution time is after the most recent matching cron time.
                    # Next scheduled run will be in the future
                    scheduled_time = self.next_scheduled_run_time()

        if scheduled_time is None:
            return None

        if restriction.latest is not None and scheduled_time > restriction.latest:
            # Over the DAG's scheduled end; don't schedule.
            return None

        start = self.data_period_start(scheduled_time)
        return DagRunInfo(
            run_after=scheduled_time,
            data_interval=DataInterval(start=start, end=scheduled_time),
        )

    def data_period_start(self, period_end: DateTime):
        return period_end - Duration(**{self.period_unit: self.period_length})

    def croniter_values(self, base_datetime=None):
        if not base_datetime:
            tz = timezone(self.timezone)
            base_datetime = now(tz)

        return [croniter(expr, base_datetime) for expr in self.cron_defs]

    def next_scheduled_run_time(self, base_datetime: DateTime = None):
        min_date = None
        tz = timezone(self.timezone)

        base_datetime_localized = base_datetime.in_timezone(tz) if base_datetime else now(tz)

        for cron in self.croniter_values(base_datetime_localized):
            next_date = cron.get_next(DateTime)

            min_date = next_date if not min_date else min(min_date, next_date)

        if min_date is None:
            return None
        return pendulum_instance(min_date)

    def previous_scheduled_run_time(self, base_datetime: DateTime = None):
        """
        Get the most recent time in the past that matches one of the cron schedules
        """
        max_date = None
        tz = timezone(self.timezone)

        base_datetime_localized = base_datetime.in_timezone(tz) if base_datetime else now(tz)

        for cron in self.croniter_values(base_datetime_localized):
            prev_date = cron.get_prev(DateTime)

            max_date = prev_date if not max_date else max(max_date, prev_date)

        if max_date is None:
            return None
        return pendulum_instance(max_date)

    def validate(self) -> None:
        if not self.cron_defs:
            raise AirflowTimetableInvalid("At least one cron definition must be present")

        if self.period_unit not in self.valid_units:
            raise AirflowTimetableInvalid(f"period_unit must be one of {self.valid_units}")

        if self.period_length < 0:
            raise AirflowTimetableInvalid("period_length must not be less than zero")

        try:
            self.croniter_values()
        except Exception as e:
            raise AirflowTimetableInvalid(str(e)) from None

    @property
    def summary(self) -> str:
        """A short summary for the timetable.

        This is used to display the timetable in the web UI. A cron expression
        timetable, for example, can use this to display the expression.
        """
        return " || ".join(self.cron_defs) + f" [TZ: {self.timezone}]"

    def serialize(self) -> Dict[str, Any]:
        """Serialize the timetable for JSON encoding.

        This is called during DAG serialization to store timetable information
        in the database. This should return a JSON-serializable dict that will
        be fed into ``deserialize`` when the DAG is deserialized.
        """
        return dict(
            cron_defs=self.cron_defs,
            timezone=self.timezone,
            period_length=self.period_length,
            period_unit=self.period_unit,
        )

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> "MultiCronTimetable":
        """Deserialize a timetable from data.

        This is called when a serialized DAG is deserialized. ``data`` will be
        whatever was returned by ``serialize`` during DAG serialization.
        """
        return cls(**data)
