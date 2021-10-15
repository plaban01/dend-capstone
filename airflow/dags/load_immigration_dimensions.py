from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator, 
    LoadTableOperator
)
from helpers import SqlQueries

s3_project_path = 's3://bucket180588/capstone_project'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 10, 10),
}

dag = DAG(
    'load_immigration_dimensions',
    default_args=default_args,
    description='Load immigration dimensions data from S3 to Redshift',
    schedule_interval=None,
    max_active_runs=1
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_countries = StageToRedshiftOperator(
    task_id='Stage_countries',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_countries',
    s3_path=f'{s3_project_path}/i94_countries.csv',
    copy_format='CSV IGNOREHEADER 1'
)

load_country_dimension = LoadTableOperator(
    task_id='Load_country_dimension',
    dag=dag,
    redshift_conn_id='redshift',
    table='dim_Country',
    sql_query=SqlQueries.dim_country_insert,
    truncate=True
)

stage_states = StageToRedshiftOperator(
    task_id='Stage_states',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_states',
    s3_path=f'{s3_project_path}/i94_states.csv',
    copy_format='CSV IGNOREHEADER 1'
)

load_state_dimension = LoadTableOperator(
    task_id='Load_state_dimension',
    dag=dag,
    redshift_conn_id='redshift',
    table='dim_States',
    sql_query=SqlQueries.dim_states_insert,
    truncate=True
)

stage_ports = StageToRedshiftOperator(
    task_id='Stage_ports',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_ports',
    s3_path=f'{s3_project_path}/i94_ports.csv',
    copy_format='CSV IGNOREHEADER 1'
)

load_port_dimension = LoadTableOperator(
    task_id='Load_port_dimension',
    dag=dag,
    redshift_conn_id='redshift',
    table='dim_Ports',
    sql_query=SqlQueries.dim_ports_insert,
    truncate=True
)

stage_airports = StageToRedshiftOperator(
    task_id='Stage_airports',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_airports',
    s3_path=f'{s3_project_path}/airport-codes_csv.csv',
    copy_format='CSV IGNOREHEADER 1'
)

load_airport_types_dimension = LoadTableOperator(
    task_id='Load_airport_types_dimension',
    dag=dag,
    redshift_conn_id='redshift',
    table='dim_AirportTypes',
    sql_query=SqlQueries.dim_airport_types_insert,
    truncate=True
)

load_airports_dimension = LoadTableOperator(
    task_id='Load_airports_dimension',
    dag=dag,
    redshift_conn_id='redshift',
    table='dim_Airports',
    sql_query=SqlQueries.dim_airports_insert,
    truncate=True
)

stage_demographics = StageToRedshiftOperator(
    task_id='Stage_demographics',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_demographics',
    s3_path=f'{s3_project_path}/us-cities-demographics.csv', 
    copy_format="CSV DELIMITER ';' IGNOREHEADER 1"
)

load_demographics = LoadTableOperator(
    task_id='Load_demographics',
    dag=dag,
    redshift_conn_id='redshift',
    table='fact_demographics',
    sql_query=SqlQueries.fact_demographics_insert,
    truncate=True
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define the task dependencies

start_operator >> [stage_countries, stage_states, stage_ports, stage_airports, stage_demographics]

stage_countries >> load_country_dimension
stage_states >> load_state_dimension
stage_ports >> load_port_dimension
stage_airports >> load_airport_types_dimension
load_airport_types_dimension >> load_airports_dimension
load_state_dimension >> load_airports_dimension
stage_demographics >> load_demographics
load_port_dimension >> load_demographics

[load_country_dimension, load_state_dimension, load_port_dimension, load_airports_dimension, load_demographics] >> end_operator