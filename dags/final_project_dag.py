from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.datetime(2023, 6, 14),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    max_active_runs=1,
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials',
        s3_bucket='hpierce-airflow-project1',
        s3_key='log_data',
        table='staging_events',
        file_format="JSON",
        json_path='s3://hpierce-airflow-project1/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials',
        s3_bucket='hpierce-airflow-project1',
        s3_key='song_data',
        table='staging_songs',
        file_format='JSON',
        json_path='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        truncate=True,
        sql=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        truncate=True,
        sql=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        truncate=True,
        sql=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        truncate=True,
        sql=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        query_inputs=[
            {
                "table": "artists",
                "column": "artistid"
            },
            {
                "table": "songplays",
                "column": "playid"
            },
            {
                "table": "songplays",
                "column": "start_time"
            },
            {
                "table": "songs",
                "column": "songid"
            },
            {
                "table": "time",
                "column": "start_time"
            },
            {
                "table": "users",
                "column": "userid"
            }
        ]
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