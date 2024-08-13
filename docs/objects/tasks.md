# Tasks
Airflow [Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)
are units of work. An Operator is a pre-defined task with specific functionality.


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
    click OrbiterBashOperator href "tasks#orbiter.objects.operators.bash.OrbiterBashOperator" "OrbiterBashOperator Documentation"

    --8<-- "orbiter/objects/operators/smtp.py:mermaid-relationships"
    class OrbiterEmailOperator["orbiter.objects.operators.smtp.OrbiterEmailOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/smtp.py:mermaid-props"
    }
    click OrbiterEmailOperator href "tasks#orbiter.objects.operators.smtp.OrbiterEmailOperator" "OrbiterEmailOperator Documentation"

  --8<-- "orbiter/objects/operators/empty.py:mermaid-relationships"
    class OrbiterEmptyOperator["orbiter.objects.operators.empty.OrbiterEmptyOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/empty.py:mermaid-props"
    }
    click OrbiterEmptyOperator href "tasks#orbiter.objects.operators.empty.OrbiterEmptyOperator" "OrbiterEmptyOperator Documentation"

    --8<-- "orbiter/objects/operators/python.py:mermaid-relationships"
    class OrbiterPythonOperator["orbiter.objects.operators.python.OrbiterPythonOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/python.py:mermaid-props"
    }
    click OrbiterPythonOperator href "tasks#orbiter.objects.operators.python.OrbiterPythonOperator" "OrbiterPythonOperator Documentation"

    --8<-- "orbiter/objects/operators/sql.py:mermaid-relationships"
    class OrbiterSQLExecuteQueryOperator["orbiter.objects.operators.sql.OrbiterSQLExecuteQueryOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/sql.py:mermaid-props"
    }
    click OrbiterSQLExecuteQueryOperator href "tasks#orbiter.objects.operators.sql.OrbiterSQLExecuteQueryOperator" "OrbiterSQLExecuteQueryOperator Documentation"

    --8<-- "orbiter/objects/operators/ssh.py:mermaid-relationships"
    class OrbiterSSHOperator["orbiter.objects.operators.ssh.OrbiterSSHOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/ssh.py:mermaid-props"
    }
    click OrbiterSSHOperator href "tasks#orbiter.objects.operators.ssh.OrbiterSSHOperator" "OrbiterSSHOperator Documentation"

    --8<-- "orbiter/objects/callbacks/__init__.py:mermaid-relationships"
    class OrbiterCallback["orbiter.objects.callbacks.OrbiterCallback"] {
        --8<-- "orbiter/objects/callbacks/__init__.py:mermaid-props"
    }
    click OrbiterCallback href "tasks#orbiter.objects.callbacks.OrbiterCallback" "OrbiterCallback Documentation"

    --8<-- "orbiter/objects/callbacks/smtp.py:mermaid-relationships"
    class OrbiterSmtpNotifierCallback["orbiter.objects.callbacks.smtp.OrbiterSmtpNotifierCallback"] {
        <<OrbiterCallback>>
        --8<-- "orbiter/objects/callbacks/smtp.py:mermaid-props"
    }
    click OrbiterSmtpNotifierCallback href "tasks#orbiter.objects.callbacks.smtp.OrbiterSmtpNotifierCallback" "OrbiterSmtpNotifierCallback Documentation"
```


::: orbiter.objects.task.OrbiterOperator

::: orbiter.objects.task.OrbiterTaskDependency

::: orbiter.objects.task.OrbiterTask

::: orbiter.objects.task_group.OrbiterTaskGroup

::: orbiter.objects.operators
    options:
        show_submodules: true

::: orbiter.objects.callbacks
    options:
        show_submodules: true
