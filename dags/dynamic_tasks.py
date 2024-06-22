from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'dynamic_tasks_dag',
    default_args=default_args,
    description='A simple dynamic DAG based on a param file',
    schedule_interval=None,
)

# Parameter file (for simplicity, included directly in the script)
params = {
    "tasks": [
        {"task_id": "task_1", "task_command": "echo 'This is task 1'"},
        {"task_id": "task_2", "task_command": "echo 'This is task 2'"},
        {"task_id": "task_3", "task_command": "echo 'This is task 3'"},
        {"task_id": "task_4", "task_command": "echo 'This is task 4'"},
        {"task_id": "task_5", "task_command": "echo 'This is task 5'"},
        {"task_id": "task_6", "task_command": "exit 1"},
        {"task_id": "task_7", "task_command": "echo 'This is task 7'"},
        {"task_id": "task_8", "task_command": "echo 'This is task 8'"},
        {"task_id": "task_9", "task_command": "echo 'This is task 9'"},
        {"task_id": "task_10", "task_command": "echo 'This is task 10'"}
    ]
}

with TaskGroup('task_group', dag=dag) as tg:
    task_list = []
    previous_task = None
    for i, task in enumerate(params['tasks']):
        task_id = task['task_id']
        task_command = task['task_command']
        
        bash_task = BashOperator(
            task_id=task_id,
            bash_command=task_command,
            dag=dag,
            retries=1,
            retry_delay=timedelta(seconds=5), 
            trigger_rule=TriggerRule.ALL_DONE if i > 0 else 'all_success'
        )
        task_list.append(bash_task)
        
        # Set task dependencies
        if previous_task:
            previous_task >> bash_task
    
        previous_task = bash_task

    # Add a final marker task within the TaskGroup, after the loop
    completion_marker = DummyOperator(
        task_id='completion_marker',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag
    )

    # Set the completion marker to be dependent on the last task in the TaskGroup
    for task in task_list:
        task >> completion_marker


# Create a DummyOperator task outside the TaskGroup
final_task = DummyOperator(
    task_id='final_task',
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

# Set the TaskGroup to trigger the final task only if all tasks inside the TaskGroup are successful
completion_marker >> final_task
