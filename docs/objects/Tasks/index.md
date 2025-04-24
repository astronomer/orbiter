# Overview
Airflow [Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)
are units of work. An Operator is a pre-defined task with specific functionality.

Operators can be looked up in the [Astronomer Registry](https://registry.astronomer.io/).

The easiest way to create an operator in a translation to [use an existing subclass of `OrbiterOperator` (e.g. `OrbiterBashOperator`)](./Operators.md).

If an `OrbiterOperator` subclass doesn't exist for your use case, you can:

1) Utilize `OrbiterTask`
    ```python
    from orbiter.objects.requirement import OrbiterRequirement
    from orbiter.objects.task import OrbiterTask
    from orbiter.rules import task_rule

    @task_rule
    def my_rule(val: dict):
        return OrbiterTask(
            task_id="my_task",
            imports=[OrbiterRequirement(
                package="apache-airflow",
                module="airflow.operators.trigger_dagrun",
                names=["TriggerDagRunOperator"],
            )],
            ...
        )
    ```

2) Create a new subclass of `OrbiterOperator`, which can be beneficial if you are using it frequently
   in separate `@task_rules`
    ```python
    from orbiter.objects.task import OrbiterOperator
    from orbiter.objects.requirement import OrbiterRequirement
    from orbiter.rules import task_rule

    class OrbiterTriggerDagRunOperator(OrbiterOperator):
        # Define the imports required for the operator, and the operator name
        imports = [
            OrbiterRequirement(
                package="apache-airflow",
                module="airflow.operators.trigger_dagrun",
                names=["TriggerDagRunOperator"],
            )
        ]
        operator: str = "PythonOperator"

        # Add fields should be rendered in the output
        render_attributes = OrbiterOperator.render_attributes + [
            ...
        ]

        # Add the fields that are required for the operator here, with their types
        # Not all Airflow Operator fields are required, just the ones you will use.
        trigger_dag_id: str
        ...

    @task_rule
    def my_rule(val: dict):
        return OrbiterTriggerDagRunOperator(...)
    ```


## Diagram
```mermaid
classDiagram
    direction LR
    --8<-- "orbiter/objects/task.py:mermaid-task-relationships"
    class OrbiterOperator["orbiter.objects.task.OrbiterOperator"] {
        --8<-- "orbiter/objects/task.py:mermaid-op-props"
    }
    click OrbiterOperator href "#orbiter.objects.task.OrbiterOperator" "OrbiterOperator Documentation"

    class OrbiterTask["orbiter.objects.task.OrbiterTask"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/task.py:mermaid-task-props"
    }
    click OrbiterTask href "#orbiter.objects.task.OrbiterTask" "OrbiterTask Documentation"

    --8<-- "orbiter/objects/callbacks/__init__.py:mermaid-relationships"
    class OrbiterCallback["orbiter.objects.callbacks.OrbiterCallback"] {
        --8<-- "orbiter/objects/callbacks/__init__.py:mermaid-props"
    }
    click OrbiterCallback href "Callbacks/#orbiter.objects.callbacks.OrbiterCallback" "OrbiterCallback Documentation"

    --8<-- "orbiter/objects/callbacks/smtp.py:mermaid-relationships"
    class OrbiterSmtpNotifierCallback["orbiter.objects.callbacks.smtp.OrbiterSmtpNotifierCallback"] {
        <<OrbiterCallback>>
        --8<-- "orbiter/objects/callbacks/smtp.py:mermaid-props"
    }
    click OrbiterSmtpNotifierCallback href "Callbacks/#orbiter.objects.callbacks.smtp.OrbiterSmtpNotifierCallback" "OrbiterSmtpNotifierCallback Documentation"
```

---

## Tasks & TaskGroups

::: orbiter.objects.task.OrbiterOperator
    options:
        heading_level: 3
::: orbiter.objects.task.OrbiterTaskDependency
    options:
        heading_level: 3
::: orbiter.objects.task.OrbiterTask
    options:
        heading_level: 3
::: orbiter.objects.task_group.OrbiterTaskGroup
    options:
        heading_level: 3
