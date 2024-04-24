from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag,task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from helpers.sql_queries import schema
from helpers.sql_queries import sqlqueries
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

S3_SONG_KEY = 'song-data'
S3_LOG_KEY = 'log_data'
S3_LOG_JSON_PATH = 's3://udacity-dend/log_json_path.json'

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email': {
        'on_retry': False
    },
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@daily'
)
def final_project():

    @task()
    def drop_and_create_tables(*args, **kwargs):
        redshift_hook = PostgresHook("redshift")
        redshift_hook.run(schema.drop_and_create_tables)

    drop_and_create_tables = drop_and_create_tables()

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket = Variable.get('s3_bucket'),
        s3_key = S3_LOG_KEY,
        s3_format=f"FORMAT AS JSON {S3_LOG_JSON_KEY}"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket = Variable.get('s3_bucket'),
        s3_key = S3_SONG_KEY,
        s3_format="JSON 'auto'"
    )

    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_fact_table',
        redshift_conn_id="redshift",
        table="songplays",
        sql=sqlqueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dim_table',
        redshift_conn_id="redshift",
        table='users',
        truncate=True,
        sql=sqlqueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table='songs',
        truncate=True,
        sql=sqlqueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table='artists',
        truncate=True,
        sql=sqlqueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table='time',
        truncate=True,
        sql=sqlqueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='data_quality_checks',
        redshift_conn_id="redshift",
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )

    end_operator = DummyOperator(task_id='end_execution')

    start_operator >> drop_and_create_tables
    drop_and_create_tables >> stage_events_to_redshift
    drop_and_create_tables >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_song_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()
