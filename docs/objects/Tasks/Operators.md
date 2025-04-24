
!!! note

    These operators are included and are intended to represent some of the most common Airflow Operators,
    but not _all_ Airflow Operators.

    Additional Operators can be created by subclassing [`OrbiterOperator`][orbiter.objects.task.OrbiterOperator]
    or using [`OrbiterTask`][orbiter.objects.task.OrbiterTask] directly.

    Review the [Astronomer Registry](https://registry.astronomer.io/) to find additional Airflow Operators.

---

```mermaid
classDiagram
    direction LR

    class OrbiterBashOperator["orbiter.objects.operators.bash.OrbiterBashOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/bash.py:mermaid-props"
    }
    click OrbiterBashOperator href "#orbiter.objects.operators.bash.OrbiterBashOperator" "OrbiterBashOperator Documentation"

    class OrbiterEmailOperator["orbiter.objects.operators.smtp.OrbiterEmailOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/smtp.py:mermaid-props"
    }
    click OrbiterEmailOperator href "#orbiter.objects.operators.smtp.OrbiterEmailOperator" "OrbiterEmailOperator Documentation"

    class OrbiterEmptyOperator["orbiter.objects.operators.empty.OrbiterEmptyOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/empty.py:mermaid-props"
    }
    click OrbiterEmptyOperator href "#orbiter.objects.operators.empty.OrbiterEmptyOperator" "OrbiterEmptyOperator Documentation"

    class OrbiterKubernetesPodOperator["orbiter.objects.operators.kubernetes_pod.OrbiterKubernetesPodOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/kubernetes_pod.py:mermaid-props"
    }
    click OrbiterKubernetesPodOperator href "#orbiter.objects.operators.kubernetes_pod.OrbiterKubernetesPodOperator" "OrbiterKubernetesPodOperator Documentation"

    class OrbiterLivyOperator["orbiter.objects.operators.livy.OrbiterLivyOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/livy.py:mermaid-props"
    }
    click OrbiterLivyOperator href "#orbiter.objects.operators.livy.OrbiterLivyOperator" "OrbiterLivyOperator Documentation"

    class OrbiterPythonOperator["orbiter.objects.operators.python.OrbiterPythonOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/python.py:mermaid-props"
    }
    click OrbiterPythonOperator href "#orbiter.objects.operators.python.OrbiterPythonOperator" "OrbiterPythonOperator Documentation"

    --8<-- "orbiter/objects/operators/python.py:mermaid-relationships"
    class OrbiterDecoratedPythonOperator["orbiter.objects.operators.python.OrbiterDecoratedPythonOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/python.py:mermaid-props"
    }
    click OrbiterDecoratedPythonOperator href "#orbiter.objects.operators.python.OrbiterDecoratedPythonOperator" "OrbiterDecoratedPythonOperator Documentation"

    class OrbiterSQLExecuteQueryOperator["orbiter.objects.operators.sql.OrbiterSQLExecuteQueryOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/sql.py:mermaid-props"
    }
    click OrbiterSQLExecuteQueryOperator href "#orbiter.objects.operators.sql.OrbiterSQLExecuteQueryOperator" "OrbiterSQLExecuteQueryOperator Documentation"

    class OrbiterSSHOperator["orbiter.objects.operators.ssh.OrbiterSSHOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/ssh.py:mermaid-props"
    }
    click OrbiterSSHOperator href "#orbiter.objects.operators.ssh.OrbiterSSHOperator" "OrbiterSSHOperator Documentation"

    class OrbiterWinRMOperator["orbiter.objects.operators.win_rm.OrbiterWinRMOperator"] {
        <<OrbiterOperator>>
        --8<-- "orbiter/objects/operators/win_rm.py:mermaid-props"
    }
    click OrbiterWinRMOperator href "#orbiter.objects.operators.win_rm.OrbiterWinRMOperator" "OrbiterWinRMOperator Documentation"
```

---

::: orbiter.objects.operators.bash.OrbiterBashOperator
::: orbiter.objects.operators.empty.OrbiterEmptyOperator
::: orbiter.objects.operators.kubernetes_pod.OrbiterKubernetesPodOperator
::: orbiter.objects.operators.livy.OrbiterLivyOperator
::: orbiter.objects.operators.python.OrbiterPythonOperator
::: orbiter.objects.operators.python.OrbiterDecoratedPythonOperator
::: orbiter.objects.operators.smtp.OrbiterEmailOperator
::: orbiter.objects.operators.sql.OrbiterSQLExecuteQueryOperator
::: orbiter.objects.operators.ssh.OrbiterSSHOperator
::: orbiter.objects.operators.win_rm.OrbiterWinRMOperator
