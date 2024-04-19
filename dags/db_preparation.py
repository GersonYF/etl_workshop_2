import pytz

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from tasks import (
    read_csv,
    read_db,
    transform_csv,
    transform_db,
    merge,
    load,
    store,
)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



dag = DAG(
    'db_preparation',
    default_args=default_args,
    description='A DAG to prepare the database for Songs and Awards data',
    schedule_interval=timedelta(days=1),
    start_date=datetime.now(pytz.utc),
    catchup=False,
)

t_read_csv = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv,
    dag=dag,
)

t_read_db = PythonOperator(
    task_id='read_db',
    python_callable=read_db,
    dag=dag,
)

t_transform_csv = PythonOperator(
    task_id='transform_csv',
    python_callable=transform_csv,
    dag=dag,
)

t_transform_db = PythonOperator(
    task_id='transform_db',
    python_callable=transform_db,
    dag=dag,
)

t_merge = PythonOperator(
    task_id='merge',
    python_callable=merge,
    dag=dag,
)

t_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

t_store = PythonOperator(
    task_id='store',
    python_callable=store,
    dag=dag,
)


t_read_db >> t_transform_db >> t_merge
t_read_csv >> t_transform_csv >> t_merge >> t_load >> t_store
