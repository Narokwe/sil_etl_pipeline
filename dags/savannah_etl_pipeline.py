# Import the necessary python packages
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Augustine',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'savannah_incremental_etl_pipeline',
    default_args=default_args,
    description='Incremental ETL Pipeline for Savannah Informatics',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['savannah', 'etl', 'incremental'],
)

def setup_infrastructure_task():
    """Task to create BigQuery dataset and tables"""
    print("Setting up BigQuery infrastructure...")
    import sys
    sys.path.append('/opt/airflow/scripts')
    from scripts.load_data import create_bq_tables_if_not_exist
    create_bq_tables_if_not_exist()
    print("BigQuery infrastructure ready!")

def extract_task():
    """Task to extract data from APIs with incremental logic"""
    print("Starting incremental data extraction from APIs...")
    import sys
    sys.path.append('/opt/airflow/scripts')
    from scripts.extract_data import extract_all_data
    return extract_all_data()

def transform_task(**kwargs):
    """Task to transform and clean data"""
    print("Starting data transformation...")
    ti = kwargs['ti']
    extraction_results = ti.xcom_pull(task_ids='extract_data')
    
    import sys
    sys.path.append('/opt/airflow/scripts')
    from scripts.transform_data import transform_all_data
    
    return transform_all_data(extraction_results)

def load_task(**kwargs):
    """Task to load data to BigQuery using incremental MERGE"""
    print("Starting incremental data loading to BigQuery...")
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data')
    
    import sys
    sys.path.append('/opt/airflow/scripts')
    from scripts.load_data import load_incremental_data
    
    load_results = load_incremental_data(transformed_data)
    print("Data loaded to BigQuery using incremental MERGE!")
    return load_results

def analyze_task():
    """Task to run analysis queries"""
    print("Starting data analysis...")
    import sys
    sys.path.append('/opt/airflow/scripts')
    from scripts.queries import run_all_analyses
    run_all_analyses()
    print("Analysis completed successfully")

# Define tasks
start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

setup_infrastructure = PythonOperator(
    task_id='setup_infrastructure',
    python_callable=setup_infrastructure_task,
    dag=dag,
)

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_task,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    provide_context=True,
    dag=dag,
)

analyze_data = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_task,
    dag=dag,
)

end_pipeline = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Define task dependencies - infrastructure first!
start_pipeline >> setup_infrastructure >> extract_data >> transform_data >> load_data >> analyze_data >> end_pipeline