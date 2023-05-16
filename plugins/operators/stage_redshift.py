from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_conn_id='',
                 s3_bucket='',
                 s3_key='',
                 region='',
                 table='',
                 file_format='',
                 json_path='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.table = table
        self.file_format = file_format
        self.json_path = json_path

    def execute(self, context):
        aws_access_key = AwsHook(self.aws_conn_id).get_credentials().access_key
        aws_secret_key = AwsHook(self.aws_conn_id).get_credentials().secret_key

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = f"""
            COPY {self.table}
            FROM 's3://{self.s3_bucket}/{self.s3_key}'
            ACCESS_KEY_ID '{aws_access_key}'
            SECRET_ACCESS_KEY '{aws_secret_key}'
            REGION '{self.region}'
            {self.file_format} '{self.json_path}'
        """

        redshift.run(formatted_sql)
        self.log.info("Finished copying data")
