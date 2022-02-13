from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers.sql_queries import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past':False,
    'start_date': datetime(2019, 1, 12),
    'email_on_retry':False,
    'email_on_failure':False,
    'retries':3,
    'catchup':False,
    'retry_delay': timedelta(minutes=5)
}

#Defining the DAG
dag = DAG('Datapipeline_with_airflow_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup = False
        )

#Defining the tasks

#Start Operator
start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

#Tasks for loading data into the staging tables from the json log files
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    table='staging_events',
    s3_path="s3://udacity-dend/log_data/",
    extra_params="format as json 's3://udacity-dend/log_json_path.json'",
    aws_conn_id="aws_credentials"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table='staging_songs',
    s3_path="s3://udacity-dend/song_data",
    extra_params="format as json 'auto' compupdate off",
    aws_conn_id="aws_credentials"
)

#Task for processing and loading the data into the fact table 
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table_name="songplays",
    sql=SqlQueries.songplay_table_insert,
    redshift_conn_id = "redshift"
)

#Tasks for processing and loading the data into the dimension tables 
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table_name="users",
    sql=SqlQueries.user_table_insert,
    redshift_conn_id = "redshift"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table_name="songs",
    sql=SqlQueries.song_table_insert,
    redshift_conn_id = "redshift"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table_name="artists",
    sql=SqlQueries.artist_table_insert,
    redshift_conn_id = "redshift"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table_name="time",
    sql=SqlQueries.time_table_insert,
    redshift_conn_id = "redshift"
)

#Task for performing data quality check 
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dataquality_checks=[
        { 'sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0 }, 
        { 'sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'expected_result': 2 },
        { 'sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'expected_result': 0 },
        { 'sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'expected_result': 0 },
        { 'sql': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'expected_result': 0 },
        { 'sql': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL', 'expected_result': 0 },
        { 'sql': 'SELECT COUNT(*) FROM public.songplays sp LEFT OUTER JOIN public.users u ON u.userid = sp.userid WHERE u.userid IS NULL', \
         'expected_result': 0 }
        ]
)

#The end task
end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

# Defining the dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator