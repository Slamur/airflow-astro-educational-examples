from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    schedule=None,
    start_date=datetime(2026, 2, 19),
    tags=['sensor', 'example'],
    catchup=False
)
def no_schedule_dag():

    @task
    def runme():
        print("Hi")

    runme()

no_schedule_dag()
