from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 query_inputs=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_inputs = query_inputs

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for query_input in self.query_inputs:
            table = query_input["table"]
            column = query_input["column"]
            null_records = redshift.get_records(f"SELECT {column} FROM {table} WHERE {column} IS NULL")
            self.log.info(f"Number of null records in column {column}: {len(null_records)}")
            if (len(null_records) > 0):
                raise ValueError(f"Data quality check failed. The table {table} has at least one null record in the column {self.column}.")
        self.log.info('Data quality check passed.')
