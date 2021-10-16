from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator, 
    LoadTableOperator,
    DataQualityOperator
)
from helpers import SqlQueries

s3_project_path = 's3://bucket180588/capstone_project'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2016, 1, 1),
    'end_date': datetime(2016, 1, 2),
}

dag = DAG(
    'load_immigration_facts',
    default_args=default_args,
    description='Load immigration facts data from S3 to Redshift',
    schedule_interval='@daily',
    max_active_runs=1
)

dimension_quality_checks = [
    { 'name': 'Check country table row count', 'sql': SqlQueries.row_count_query.format(table="dim_Country"), 'condition': 'result > 0' },
    { 'name': 'Check state table row count', 'sql': SqlQueries.row_count_query.format(table="dim_States"), 'condition': 'result > 0' },
    { 'name': 'Check ports table row count', 'sql': SqlQueries.row_count_query.format(table="dim_Ports"), 'condition': 'result > 0' },
    { 'name': 'Check airports table row count', 'sql': SqlQueries.row_count_query.format(table="dim_Airports"), 'condition': 'result > 0' }
]

immigration_quality_checks = [
    { 'name': 'Check immigration row count', 'sql': SqlQueries.immigration_row_count, 'condition': 'result > 0' }
]

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_immigration = StageToRedshiftOperator(
    task_id='Stage_immigration',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_immigration',
    s3_path="{{ execution_date.strftime('s3://bucket180588/capstone_project/immigration.parquet/year=%Y/month=%-m/arrival_day=%-d/') }}",
    copy_format='PARQUET'
)

load_travel_modes_dimension = LoadTableOperator(
    task_id='Load_travel_modes_dimension',
    dag=dag,
    redshift_conn_id='redshift',
    table='dim_TravelModes',
    sql_query=SqlQueries.dim_travelmodes_insert
)

load_visa_types_dimension = LoadTableOperator(
    task_id='Load_visa_types_dimension',
    dag=dag,
    redshift_conn_id='redshift',
    table='dim_VisaTypes',
    sql_query=SqlQueries.dim_visatypes_insert
)

load_time_dimension = LoadTableOperator(
    task_id='Load_time_dimension',
    dag=dag,
    redshift_conn_id='redshift',
    table='dim_time',
    sql_query=SqlQueries.dim_time_insert
)

check_dimension_data_quality = DataQualityOperator(
    task_id='Run_dimension_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    checks=dimension_quality_checks
)

load_immigration = LoadTableOperator(
    task_id='Load_immigration',
    dag=dag,
    redshift_conn_id='redshift',
    table='fact_immigration',
    sql_query=SqlQueries.fact_immigration_insert
)

check_immigration_data_quality = DataQualityOperator(
    task_id='Run_immigration_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    checks=immigration_quality_checks
)

end_operator = DummyOperator(task_id='End_execution', dag=dag)

# Define task dependencies
start_operator >> stage_immigration

stage_immigration >> [load_travel_modes_dimension, load_visa_types_dimension, load_time_dimension]
[load_travel_modes_dimension, load_visa_types_dimension, load_time_dimension] >> check_dimension_data_quality
check_dimension_data_quality >> load_immigration >> check_immigration_data_quality

check_immigration_data_quality >> end_operator