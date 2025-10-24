from orbiter.objects.timetables.multiple_cron_trigger_timetable import OrbiterMultipleCronTriggerTimetable


def test_multiple_cron_trigger_timetable_serde():
    test = OrbiterMultipleCronTriggerTimetable(
        task_id="task_id",
        crons=["*/5 * * * *", "1 * * * *"],
        timezone="America/New_York",
    )
    actual_json = test.model_dump_json()
    actual = OrbiterMultipleCronTriggerTimetable.model_validate_json(actual_json)
    assert actual == test
