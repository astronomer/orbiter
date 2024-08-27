# Workflow

Airflow workflows are represented by a
[DAG](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
which is a Directed Acyclic Graph (of [Tasks](../tasks)).

## Diagram
```mermaid
classDiagram
    direction LR
    class OrbiterRequirement["orbiter.objects.requirement.OrbiterRequirement"] {
        --8<-- "orbiter/objects/requirement.py:mermaid-props"
    }

    --8<-- "orbiter/objects/dag.py:mermaid-dag-relationships"
    class OrbiterDAG["orbiter.objects.dag.OrbiterDAG"] {
        --8<-- "orbiter/objects/dag.py:mermaid-props"
    }
    click OrbiterDAG href "#orbiter.objects.dag.OrbiterDAG" "OrbiterDAG Documentation"

    --8<-- "orbiter/objects/task_group.py:mermaid-dag-relationships"
    class OrbiterTaskGroup["orbiter.objects.task.OrbiterTaskGroup"] {
        --8<-- "orbiter/objects/task_group.py:mermaid-op-props"
    }

    --8<-- "orbiter/objects/task.py:mermaid-dag-relationships"
    class OrbiterOperator["orbiter.objects.task.OrbiterOperator"] {
        --8<-- "orbiter/objects/task.py:mermaid-op-props"
    }

    class OrbiterTimetable["orbiter.objects.timetables.OrbiterTimetable"] {
        --8<-- "orbiter/objects/timetables/__init__.py:mermaid-props"
    }
    click OrbiterTimetable href "#orbiter.objects.timetables.OrbiterTimetable" "OrbiterTimetable Documentation"

    class OrbiterConnection["orbiter.objects.connection.OrbiterConnection"] {
        --8<-- "orbiter/objects/connection.py:mermaid-props"
    }

    class OrbiterEnvVar["orbiter.objects.env_var.OrbiterEnvVar"] {
        --8<-- "orbiter/objects/env_var.py:mermaid-props"
    }

    class OrbiterPool["orbiter.objects.pool.OrbiterPool"] {
        --8<-- "orbiter/objects/pool.py:mermaid-props"
    }

    class OrbiterRequirement["orbiter.objects.requirement.OrbiterRequirement"] {
        --8<-- "orbiter/objects/requirement.py:mermaid-props"
    }

    class OrbiterVariable["orbiter.objects.variable.OrbiterVariable"] {
        --8<-- "orbiter/objects/variable.py:mermaid-props"
    }
```

::: orbiter.objects.dag.OrbiterDAG

## Timetables
::: orbiter.objects.timetables
    options:
        heading_level: 3
        show_submodules: true
        show_object_full_path: true
        show_root_heading: false
        show_root_toc_entry: false
