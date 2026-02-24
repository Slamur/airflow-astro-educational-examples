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

    @task
    def download_file(folder: str, file_name: str):
        print(f"Downloading {file_name} to {folder}...")

    files = download_file.partial(folder="/downloads").expand(file_name=get_files())

dynamic_tasks_dag()