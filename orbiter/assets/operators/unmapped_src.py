from airflow.operators.empty import EmptyOperator


class UnmappedOperator(EmptyOperator):
    def __init__(self, task_id: str, source: str, **kwargs):
        super().__init__(task_id, **kwargs)
        self.source = source
