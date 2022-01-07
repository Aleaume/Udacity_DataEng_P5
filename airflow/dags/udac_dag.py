from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)

from helpers import SqlQueries



# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    #'start_date': datetime(2019, 1, 12)
    
}

dag = DAG('udac_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )


dag_Delete = DAG('delete_all',
          default_args=default_args,
          description='Delete all Tables from Redshift'
        )

delete_tables = PostgresOperator(
    task_id="delete_tables",
    dag=dag_Delete,
    postgres_conn_id="redshift",
    sql=SqlQueries.drop_all_tables
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_tables = PostgresOperator(
    task_id="create_staging_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=[SqlQueries.create_songs_table,SqlQueries.create_events_table]
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/",
    provide_context=True
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A/",
    provide_context=True
)

create_star_tables = PostgresOperator(
    task_id="create_star_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=[SqlQueries.create_table_songplays,
         SqlQueries.create_table_users,
         SqlQueries.create_table_songs,
         SqlQueries.create_table_artists,
         SqlQueries.create_table_time]
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql = SqlQueries.songplay_table_insert,
    table = "songplays",
    truncate = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql = SqlQueries.user_table_insert,
    truncate = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql = SqlQueries.song_table_insert,
    truncate = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql = SqlQueries.artist_table_insert,
    truncate = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql = SqlQueries.time_table_insert,
    truncate = True
)

run_quality_checks_songs = DataQualityOperator(
    task_id='Run_data_quality_checks_songs',
    dag=dag,
    redshift_conn_id="redshift",
    insert_query = SqlQueries.song_table_insert,
    table_star = "songs"
)

run_quality_checks_artists = DataQualityOperator(
    task_id='Run_data_quality_checks_artists',
    dag=dag,
    redshift_conn_id="redshift",
    insert_query = SqlQueries.artist_table_insert,
    table_star = "artists"
)

run_quality_checks_songplays = DataQualityOperator(
    task_id='Run_data_quality_checks_songplays',
    dag=dag,
    redshift_conn_id="redshift",
    insert_query = SqlQueries.songplay_table_insert,
    table_star = "songplays"
)

run_quality_checks_users = DataQualityOperator(
    task_id='Run_data_quality_checks_users',
    dag=dag,
    redshift_conn_id="redshift",
    insert_query = SqlQueries.user_table_insert,
    table_star = "users"
)

run_quality_checks_time = DataQualityOperator(
    task_id='Run_data_quality_checks_time',
    dag=dag,
    redshift_conn_id="redshift",
    insert_query = SqlQueries.time_table_insert,
    table_star = "time"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator >> create_staging_tables

create_staging_tables >> stage_events_to_redshift
create_staging_tables >> stage_songs_to_redshift

stage_songs_to_redshift >> create_star_tables 
stage_events_to_redshift >> create_star_tables

create_star_tables >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks_users
load_song_dimension_table >> run_quality_checks_songs 
load_artist_dimension_table >> run_quality_checks_artists
load_time_dimension_table >> run_quality_checks_time
load_songplays_table >> run_quality_checks_songplays

run_quality_checks_songs >> end_operator
run_quality_checks_time >> end_operator
run_quality_checks_artists >> end_operator
run_quality_checks_users >> end_operator
run_quality_checks_songplays >> end_operator
