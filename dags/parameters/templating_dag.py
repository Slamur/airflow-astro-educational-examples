from airflow.sdk import dag, task, get_current_context
from pendulum import datetime

@dag(
    start_date=datetime(2026, 2, 25),
    schedule=None,
    catchup=False,
    tags=["example", "templating"],
)
def templating_dag():

    @task
    def python_task():
        context = get_current_context()

        ids = context['dag_run'].conf.get('ids', [])

        ids_sum = sum(ids, 0)
        print(f"The sum of the ids is: {ids_sum}")

    python_task()

templating_dag()