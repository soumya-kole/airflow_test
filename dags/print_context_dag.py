from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess

def print_context(**kwargs):
    """
    Print the task_id and execution_date for context.
    """
    # print('ds', ds)
    fq = kwargs['params']['query']
    print('Final Query ======', fq)
    result = subprocess.run(fq, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    
    # Print the output
    print("Output:", result.stdout)
    print("Error:", result.stderr)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'print_context_dag',
    default_args=default_args,
    description='A simple DAG with a PythonOperator',
    schedule_interval=None,
) as dag:

    print_context_task = PythonOperator(
        task_id='print_context',
        python_callable=print_context,
        provide_context=True
        )

    print_context_task
