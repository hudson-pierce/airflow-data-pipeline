from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries

from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = S3ToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials',
        s3_bucket='hpierce-airflow-project1',
        s3_key='log_data',
        schema='PUBLIC',
        table='staging_events',
        copy_options=["FORMAT AS JSON 'auto'"],
    )

    stage_songs_to_redshift = S3ToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials',
        s3_bucket='hpierce-airflow-project1',
        s3_key='song_data',
        schema='PUBLIC',
        table='staging_songs',
        copy_options=["FORMAT AS JSON 'auto'"],
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()