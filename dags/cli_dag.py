from airflow.sdk import dag, task
from pendulum import datetime

@dag(
        start_date=datetime(2026, 2, 22),
        schedule=None, 
        catchup=False,
        tags=['backfill-trigger-cli', 'example'],
)
def backfill_trigger_cli_dag():

    # Use the UI to trigger a DAG run with conf to trigger a backfill, passing in start/end dates and dag_id etc:
    @task.bash
    def trigger_backfill(**context):
        conf = context['dag_run'].conf
        return (
            f"airflow backfill create "
            f"--dag-id {conf['dag_id']} "
            f"--from-date {conf['date_start']} "
            f"--to-date {conf['date_end']} "
            f"--reprocess-behavior completed"
        )
    
    trigger_backfill()

backfill_trigger_cli_dag()

"""
airflow dags test backfill_trigger_cli_dag 2026-02-22 --conf '{ "date_start": "2026-02-22", "date_end": "2026-02-23", "dag_id": "check_dag"}'
"""