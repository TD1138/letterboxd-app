from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from daily_update import daily_update

default_args = {
    'owner': 'TD1138',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 12),
    'retries': 1,
}

dag = DAG('daily_update', default_args=default_args, schedule_interval='@daily')

t1 = PythonOperator(task_id='run_daily_update', python_callable=daily_update, dag=dag)
t1