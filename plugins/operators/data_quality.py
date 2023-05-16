from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for check in self.checks:
            test_sql = check['test_sql']
            expected_result = check['expected_result']
            records = redshift.get_records(test_sql)
            if len(records) > expected_result:
                raise ValueError(f"Data quality check failed for query: {test_sql}")
        self.log.info('Data quality check passed.')
