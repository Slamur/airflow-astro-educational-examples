from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import FileSensor
from pendulum import datetime

@dag(
    schedule=None,
    start_date=datetime(2026, 2, 19),
    tags=['sensor', 'example'],
    catchup=False
)
def sensors_dag():

    files_cnt = 3

    filepaths = [
        f'data_{i}.csv' 
        for i in range(1, 1 + files_cnt)
    ]

    wait_for_files = FileSensor.partial(
        task_id='wait_for_files',
        fs_conn_id='fs_default',
        mode='reschedule',
    ).expand(
        filepath=filepaths,
    )

    @task
    def process_file():
        print("I processed the file!")

    wait_for_files >> process_file()

sensors_dag()
