# Workflow

Airflow workflows are represented by a
[DAG](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
which is a Directed Acyclic Graph (of [Tasks](./Tasks/index.md)).

## Diagram
```mermaid
classDiagram
    direction LR
    --8<-- "orbiter/objects/dag.py:mermaid-dag-relationships"
    class OrbiterDAG["orbiter.objects.dag.OrbiterDAG"] {
        --8<-- "orbiter/objects/dag.py:mermaid-props"
    }
    click OrbiterDAG href "#orbiter.objects.dag.OrbiterDAG" "OrbiterDAG Documentation"

    --8<-- "orbiter/objects/task_group.py:mermaid-dag-relationships"
    class OrbiterTaskGroup["orbiter.objects.task.OrbiterTaskGroup"] {
        --8<-- "orbiter/objects/task_group.py:mermaid-op-props"
    }
    click OrbiterTaskGroup href "../tasks/#orbiter.objects.task_group.OrbiterTaskGroup" "OrbiterTaskGroup Documentation"

    --8<-- "orbiter/objects/task.py:mermaid-dag-relationships"
    class OrbiterOperator["orbiter.objects.task.OrbiterOperator"] {
        --8<-- "orbiter/objects/task.py:mermaid-op-props"
    }
    click OrbiterOperator href "../tasks/#orbiter.objects.task.OrbiterOperator" "OrbiterOperator Documentation"

    --8<-- "orbiter/objects/timetables/__init__.py:mermaid-dag-relationships"
    class OrbiterTimetable["orbiter.objects.timetables.OrbiterTimetable"] {
        --8<-- "orbiter/objects/timetables/__init__.py:mermaid-props"
    }
    click OrbiterTimetable href "#orbiter.objects.timetables.OrbiterTimetable" "OrbiterTimetable Documentation"

    --8<-- "orbiter/objects/timetables/multiple_cron_trigger_timetable.py:mermaid-dag-relationships"
    class OrbiterMultipleCronTriggerTimetable["orbiter.objects.timetables.multiple_cron_trigger_timetable.OrbiterMultipleCronTriggerTimetable"] {
        --8<-- "orbiter/objects/timetables/multiple_cron_trigger_timetable.py:mermaid-props"
    }
    click OrbiterMultipleCronTriggerTimetable href "#orbiter.objects.timetables.multiple_cron_trigger_timetable.OrbiterMultipleCronTriggerTimetable" "OrbiterMultipleCronTriggerTimetable Documentation"

    class OrbiterConnection["orbiter.objects.connection.OrbiterConnection"] {
        --8<-- "orbiter/objects/connection.py:mermaid-props"
    }
    click OrbiterConnection href "../project/#orbiter.objects.connection.OrbiterConnection" "OrbiterConnection Documentation"

    class OrbiterEnvVar["orbiter.objects.env_var.OrbiterEnvVar"] {
        --8<-- "orbiter/objects/env_var.py:mermaid-props"
    }
    click OrbiterEnvVar href "../project/#orbiter.objects.env_var.OrbiterEnvVar" "OrbiterEnvVar Documentation"

    class OrbiterPool["orbiter.objects.pool.OrbiterPool"] {
        --8<-- "orbiter/objects/pool.py:mermaid-props"
    }
    click OrbiterPool href "../project/#orbiter.objects.pool.OrbiterPool" "OrbiterPool Documentation"

    class OrbiterRequirement["orbiter.objects.requirement.OrbiterRequirement"] {
        --8<-- "orbiter/objects/requirement.py:mermaid-props"
    }
    click OrbiterRequirement href "../project/#orbiter.objects.requirement.OrbiterRequirement" "OrbiterDAG Documentation"

    class OrbiterVariable["orbiter.objects.variable.OrbiterVariable"] {
        --8<-- "orbiter/objects/variable.py:mermaid-props"
    }
    click OrbiterVariable href "../project/#orbiter.objects.variable.OrbiterVariable" "OrbiterVariable Documentation"
```

::: orbiter.objects.dag.OrbiterDAG
    options:
        heading_level: 3

## Timetables
::: orbiter.objects.timetables.OrbiterTimetable
    options:
        heading_level: 3
::: orbiter.objects.timetables.orbiter_multiple_cron_trigger_timetable.OrbiterMultipleCronTriggerTimetable
    options:
        heading_level: 3
