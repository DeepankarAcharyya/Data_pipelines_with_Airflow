from datetime import datetime, timedelta
from msilib.schema import tables
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from plugins.helpers.sql_queries import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'email_on_retry':False,
    'catchup_by_default':False
}

#Defining the DAG

dag = DAG('Datapipeline_with_airflow_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup = False
        )

#Defining the operators

#Start Operator
start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

#
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    table='staging_events',
    s3_path="s3://udacity-dend/log_data",
    aws_conn_id="aws_credentials"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table='staging_songs',
    s3_path="s3://udacity-dend/log_data",
    aws_conn_id="aws_credentials"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table_name="songplay_table",
    sql=SqlQueries.songplay_table_insert,
    redshift_conn_id = "redshift"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table_name="user_dimension_table",
    sql=SqlQueries.user_table_insert,
    redshift_conn_id = "redshift"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table_name="song_dimension_table",
    sql=SqlQueries.song_table_insert,
    redshift_conn_id = "redshift"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table_name="artist_dimension_table",
    sql=SqlQueries.artist_table_insert,
    redshift_conn_id = "redshift"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table_name="time_dimension_table",
    sql=SqlQueries.time_table_insert,
    redshift_conn_id = "redshift"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=['staging_events',
            'staging_songs',
            "songplay_table",
            "user_dimension_table",
            "song_dimension_table",
            "artist_dimension_table",
            "time_dimension_table"]
)

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