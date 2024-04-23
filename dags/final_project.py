from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag,task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from stage_redshift import StageToRedshiftOperator
from load_fact import LoadFactOperator
from load_dimension import LoadDimensionOperator
from data_quality import DataQualityOperator
from sql_files.sql_queries import createSchema
from sql_files.final_project_sql_statements import SqlQueries
from airflow.models import Variable

S3_SONG_KEY = 'song_data'
S3_LOG_KEY = 'log_data/{execution_date.year}/{execution_date.month}'
LOG_JSON_PATH = f's3://{S3_BUCKET}/log_json_path.json'

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
    def create_schema(*args, **kwargs):
        redshift_hook = PostgresHook("redshift")
        redshift_hook.run(createSchema.create)

    create_schema = create_schema()

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_Events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket = Variable.get('s3_bucket'),
        s3_key = "log-data",
        s3_format="FORMAT AS JSON 's3://udacity-dend/log_data'"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket = Variable.get('s3_bucket'),
        s3_key = "song-data/A/A/A",
        s3_format="JSON 'auto'"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table="songplays",
        sql=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table='users',
        truncate=True,
        sql=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table='songs',
        truncate=True,
        sql=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table='artists',
        truncate=True,
        sql=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table='time',
        truncate=True,
        sql=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )

    end_operator = DummyOperator(task_id='End_execution')

    create_schema >> start_operator
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

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
