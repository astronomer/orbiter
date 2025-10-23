from airflow.operators.empty import EmptyOperator


class UnmappedOperator(EmptyOperator):
    def __init__(self, source: str, task_id: str, **kwargs):
        super().__init__(task_id, **kwargs)
        self.source = source
