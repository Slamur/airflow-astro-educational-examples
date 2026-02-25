from airflow.sdk import dag, task, Variable
from pendulum import datetime

@dag(
    schedule=None,
    start_date=datetime(2026, 2, 23),
    tags=['variable', 'example'],
    catchup=False,
)
def variables_dag():

    @task
    def settings_variable():
        var_from_settings = Variable.get("settings_example_variable")
        print(f"Variable from settings: {var_from_settings}")

    @task
    def env_variable():
        var_from_env = Variable.get("example_variable", deserialize_json=True)
        print(f"Variable from environment: {var_from_env}")

    settings_variable()
    env_variable()

variables_dag()

"""
AIRFLOW_VAR_EXAMPLE_VARIABLE='{"env_example_key": "env_example_value"}'
"""