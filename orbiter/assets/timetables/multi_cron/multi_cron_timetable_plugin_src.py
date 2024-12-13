from airflow.plugins_manager import AirflowPlugin
from include.multi_cron_timetable import MultiCronTimetable


class MultiCronTimetablePlugin(AirflowPlugin):
    name = "multi_cron_timetable"
    timetables = [MultiCronTimetable]
