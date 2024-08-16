# Overview
Airflow [Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)
are units of work. An Operator is a pre-defined task with specific functionality.

Operators can be looked up in the [Astronomer Registry](https://registry.astronomer.io/).

The easiest way to utilize an operator is to use a subclass of `OrbiterOperator` (e.g. `OrbiterBashOperator`).

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

2) Create a new subclass of `OrbiterOperator` (beneficial if you are using it frequently in separate `@task_rules`)
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
    click OrbiterOperator href "tasks#orbiter.objects.task.OrbiterOperator" "OrbiterOperator Documentation"

    class OrbiterTask["orbiter.objects.task.OrbiterTask"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/task.py:mermaid-task-props"
    }
    click OrbiterTask href "tasks#orbiter.objects.task.OrbiterTask" "OrbiterTask Documentation"

    --8<-- "orbiter/objects/operators/bash.py:mermaid-relationships"
    class OrbiterBashOperator["orbiter.objects.operators.bash.OrbiterBashOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/bash.py:mermaid-props"
    }
    click OrbiterBashOperator href "Operators_and_Callbacks/operators#orbiter.objects.operators.bash.OrbiterBashOperator" "OrbiterBashOperator Documentation"

    --8<-- "orbiter/objects/operators/smtp.py:mermaid-relationships"
    class OrbiterEmailOperator["orbiter.objects.operators.smtp.OrbiterEmailOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/smtp.py:mermaid-props"
    }
    click OrbiterEmailOperator href "Operators_and_Callbacks/operators#orbiter.objects.operators.smtp.OrbiterEmailOperator" "OrbiterEmailOperator Documentation"

  --8<-- "orbiter/objects/operators/empty.py:mermaid-relationships"
    class OrbiterEmptyOperator["orbiter.objects.operators.empty.OrbiterEmptyOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/empty.py:mermaid-props"
    }
    click OrbiterEmptyOperator href "Operators_and_Callbacks/operators#orbiter.objects.operators.empty.OrbiterEmptyOperator" "OrbiterEmptyOperator Documentation"

    --8<-- "orbiter/objects/operators/python.py:mermaid-relationships"
    class OrbiterPythonOperator["orbiter.objects.operators.python.OrbiterPythonOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/python.py:mermaid-props"
    }
    click OrbiterPythonOperator href "Operators_and_Callbacks/operators#orbiter.objects.operators.python.OrbiterPythonOperator" "OrbiterPythonOperator Documentation"

    --8<-- "orbiter/objects/operators/sql.py:mermaid-relationships"
    class OrbiterSQLExecuteQueryOperator["orbiter.objects.operators.sql.OrbiterSQLExecuteQueryOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/sql.py:mermaid-props"
    }
    click OrbiterSQLExecuteQueryOperator href "Operators_and_Callbacks/operators#orbiter.objects.operators.sql.OrbiterSQLExecuteQueryOperator" "OrbiterSQLExecuteQueryOperator Documentation"

    --8<-- "orbiter/objects/operators/ssh.py:mermaid-relationships"
    class OrbiterSSHOperator["orbiter.objects.operators.ssh.OrbiterSSHOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/ssh.py:mermaid-props"
    }
    click OrbiterSSHOperator href "Operators_and_Callbacks/operators#orbiter.objects.operators.ssh.OrbiterSSHOperator" "OrbiterSSHOperator Documentation"

    --8<-- "orbiter/objects/callbacks/__init__.py:mermaid-relationships"
    class OrbiterCallback["orbiter.objects.callbacks.OrbiterCallback"] {
        --8<-- "orbiter/objects/callbacks/__init__.py:mermaid-props"
    }
    click OrbiterCallback href "Operators_and_Callbacks/callbacks#orbiter.objects.callbacks.OrbiterCallback" "OrbiterCallback Documentation"

    --8<-- "orbiter/objects/callbacks/smtp.py:mermaid-relationships"
    class OrbiterSmtpNotifierCallback["orbiter.objects.callbacks.smtp.OrbiterSmtpNotifierCallback"] {
        <<OrbiterCallback>>
        --8<-- "orbiter/objects/callbacks/smtp.py:mermaid-props"
    }
    click OrbiterSmtpNotifierCallback href "Operators_and_Callbacks/callbacks#orbiter.objects.callbacks.smtp.OrbiterSmtpNotifierCallback" "OrbiterSmtpNotifierCallback Documentation"
```

::: orbiter.objects.task.OrbiterOperator
::: orbiter.objects.task.OrbiterTaskDependency
::: orbiter.objects.task.OrbiterTask
::: orbiter.objects.task_group.OrbiterTaskGroup
