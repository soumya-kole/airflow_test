import sys
import time
from time import sleep
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow import settings
from airflow.models import DagModel
from airflow.configuration import conf
def dag_success_alert(context):
    print(f"DAG has succeeded, run_id: {context['run_id']}")

def task_failure_alert(context):

    print("***DAG failed!! do something***")
    print(conf.get('webserver', 'base_url'))
    print(f"The DAG email: {context['params']['email']}")
    # log_url = f"""{context['params']}/log?dag_id={context['dag']}}&task_id={}&execution_date={}"""
    dag_run = context.get("dag_run")
    for task_instance in dag_run.get_task_instances():
        if task_instance.state == 'failed':
            log_url = dag_run.get_task_instance(context['task_instance'].task_id).log_url
            print(f"{task_instance.task_id} failed inside {context['dag'].dag_id} Log url => {log_url}")

    # print(f"The DAG context: {context}")
    # print(f"The DAG run: {dir(context['dag_run'])}")
    # print(f"DAG args: {context['conf'].as_dict()}")

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'params': {
        'email' : 'soumya',
        'purpose': 'This task is responsible for sanity check'
    }
}

# Define the DAG
dag = DAG(
    'example_dag_with_callback',
    default_args=default_args,
    description='An example DAG with dependencies',
    schedule_interval=None,
    start_date=days_ago(1),
    on_success_callback=None,
    on_failure_callback=task_failure_alert,
    catchup=False,
    doc_md= """
    This DAG does something important.
    """,
)

# Define Python functions for the tasks
def task1_func():
    print("Task 1 is completed")
    sleep(2)

def task2_func():
    print("Task 2 is completed")
    sleep(1)

def task3_func():
    print("Task 3 is completed")
    sys.exit(1)

def task4_func():
    print("Task 4 is completed")
    sleep(1)

def task5_func():
    print("Task 5 is completed")
    sys.exit(1)


# Define the tasks
task1 = PythonOperator(
    task_id='task1',
    python_callable=task1_func,
    dag=dag,
    on_success_callback=dag_success_alert
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=task2_func,
    dag=dag,
)

task3 = PythonOperator(
    task_id='task3',
    python_callable=task3_func,
    dag=dag,
)

task4 = PythonOperator(
    task_id='task4',
    python_callable=task4_func,
    dag=dag,
)

task5 = PythonOperator(
    task_id='task5',
    python_callable=task5_func,
    dag=dag,
)

# Set up the task dependencies
task1 >> task3
task2 >> task3
task3 >> [task4, task5]
