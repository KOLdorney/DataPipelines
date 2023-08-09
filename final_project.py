from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='hairflow',
        s3_key='log_data',
        json_path_option='s3://hairflow/log_json_path.json',
        table='staging_events'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket='hairflow',
        s3_key='song_data',
        json_path_option='s3://hairflow/song_json_path.json',
        table='staging_events'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table='songplays',
        sql=final_project_sql_statements.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table='user',
        sql=final_project_sql_statements.user_table_insert,
        trunc=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table='song',
        sql=final_project_sql_statements.song_table_insert,
        trunc=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table='artist',
        sql=final_project_sql_statements.artist_table_insert,
        trunc=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table='time',
        sql=final_project_sql_statements.time_table_insert,
        trunc=True
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        sql_check="SELECT COUNT(*) FROM users",
        expected="150"
    )

    end_operator = DummyOperator(task_id='End_execution')

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

final_project_dag = final_project()
