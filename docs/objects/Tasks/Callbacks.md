Airflow [callback functions](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html)
are often used to send emails, slack messages, or other [notifications](https://airflow.apache.org/docs/apache-airflow-providers/core-extensions/notifications.html) when a task fails, succeeds, or is retried.
They can also run any general Python function.

::: orbiter.objects.callbacks.smtp.OrbiterSmtpNotifierCallback
    options:
        heading_level: 3
