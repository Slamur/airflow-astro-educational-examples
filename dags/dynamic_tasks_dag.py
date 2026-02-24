from airflow.sdk import dag, task
from pendulum import datetime

import random

@dag(
    start_date=datetime(2026, 2, 24),
    schedule=None,
    catchup=False,
    tags=["dynamic_tasks", "example"],
)
def dynamic_tasks_dag():
    @task
    def get_files():
        return [f"file_{i}.txt" for i in range(1, random.randint(3, 5))]

    @task(max_active_tis_per_dag=3) # limits for parallel runs
    def download_file(folder: str, file_name: str):
        return f"{folder}/{file_name}"

    files = download_file.partial(folder="/downloads").expand(file_name=get_files())

    @task.bash
    def process_file(file_name: str):
        return f"ls {file_name}; exit 0"
    
    process_file.expand(file_name=files)

    @task.bash
    def print_files(ti):
        # gathers all the file names from the download_file tasks using XComs
        file_names = ti.xcom_pull(task_ids="download_file", dag_id="dynamic_tasks_dag", key="return_value")
        return f"echo {file_names}; exit 0"

    files >> print_files()

dynamic_tasks_dag()

"""
AIRFLOW__CORE__MAX_MAP_LENGTH=1024
"""