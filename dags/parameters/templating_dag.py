from airflow.sdk import dag, task, get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from pendulum import datetime

@dag(
    start_date=datetime(2026, 2, 25),
    schedule=None,
    catchup=False,
    tags=["example", "templating"],
    render_template_as_native_obj=True,
    template_searchpath=['include/templating/scripts/']
)
def templating_dag():

    def _get_ids_from_context():
        context = get_current_context()
        return context['dag_run'].conf.get('ids', [])

    @task
    def python_task():
        ids = _get_ids_from_context()
        ids_sum = sum(ids, 0)
        print(f"The sum of the ids is: {ids_sum}")

    @task.bash
    def bash_task():
        ids = _get_ids_from_context()
        ids_str = ",".join(map(str, ids))
        return f"bash $AIRFLOW_HOME/include/templating/scripts/script.sh {ids_str}"

    SQLExecuteQueryOperator(
        task_id="sql_task",
        conn_id="my_db",
        sql="query.sql",
        params={"ids": "{{ dag_run.conf['ids'] }}"},
    )

    python_task()
    bash_task()

templating_dag()