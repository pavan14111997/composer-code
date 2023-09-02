from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.dates import days_ago

# Define default_args and DAG
default_args = {
    'owner': 'pavan_royal',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),  # Adjust as needed
    catchup=False,
)

# Define task: Load CSV from GCS to Stage Table
load_to_stage_task = GCSToBigQueryOperator(
    task_id='load_to_stage',
    bucket='normal_bucket7',
    source_objects=['customers.csv'],  # Assume customers.csv is directly in the bucket root
    destination_project_dataset_table='pavan7763.stage_dataset.stage_table',
    schema_fields=[ 
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
    ], 
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    field_delimiter=',',
    dag=dag,
)

# Define task: Move CSV from Normal Bucket to Archival Bucket
move_to_archival_task = GCSToGCSOperator(
    task_id='move_to_archival',
    source_bucket='normal_bucket7',
    source_object='customers.csv',
    destination_bucket='archival_bucket',
    destination_object='customers.csv',
    move_object=True,  # Move the file instead of copying
    dag=dag,
)

# Set task dependencies
load_to_stage_task >> move_to_archival_task 
