from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'TT',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'catchup': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 3
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='ETL pipeline from S3 to Redshift for Sparkify songplay data',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    table='staging_events',
    create_table_query=SqlQueries.create_staging_events,
    region='us-west-2',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_format='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials='aws_credentials',
    table='staging_songs',
    create_table_query=SqlQueries.create_staging_songs,
    region='us-west-2',
    s3_bucket='udacity-dend',
    s3_key='song_data'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    create_table_query=SqlQueries.create_songplays_table,
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='user_table',
    create_table_query=SqlQueries.create_user_table,
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs_table',
    create_table_query=SqlQueries.create_songs_table,
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists_table',
    create_table_query=SqlQueries.create_artists_table,
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time_table',
    create_table_query=SqlQueries.create_time_table,
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    test_sql_query=SqlQueries.test_sql_query,
    exp_result=0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table    
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
