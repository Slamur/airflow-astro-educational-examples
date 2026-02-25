from airflow.sdk import dag, task, get_current_context
from pendulum import datetime


@dag(
    start_date=datetime(2026, 2, 19),
    schedule="@daily",
    description="DAG with XComs",
    tags=["example", "xcom"],
)
def xcom_dag():

    @task
    def push_in_context() -> None:
        context = get_current_context()
        value = "Hello from XCom!"
        context["ti"].xcom_push(key="xcom_key", value=value)

    @task
    def get_from_context() -> None:
        context = get_current_context()
        value = context["ti"].xcom_pull(key="xcom_key", task_ids="push_in_context")
        print(f"Value retrieved from XCom: '{value}'")

    @task
    def second_push_same_key(ti) -> None:
        value = "Hello again from XCom!"
        ti.xcom_push(key="xcom_key", value=value)

    @task
    def extract_multiple_values_same_key(ti) -> None:
        values = ti.xcom_pull(key="xcom_key", task_ids=["push_in_context", "second_push_same_key"])
        print(f"Values retrieved from multiple tasks: {values}")

    @task
    def push_dict(ti) -> None:
        value = {
            "message": "Hello from a dictionary in XCom!",
            "number": 42,
        }
        ti.xcom_push(key="dict_key", value=value)

    @task
    def get_dict(ti) -> None:
        value = ti.xcom_pull(key="dict_key", task_ids="push_dict")
        print(f"Dictionary retrieved from XCom: {value}")
    
    @task
    def push_in_ti(ti) -> None:
        value = "Hello from Task Instance!"
        ti.xcom_push(key="ti_key", value=value)

    @task
    def get_from_ti(ti) -> None:
        value = ti.xcom_pull(key="ti_key", task_ids="push_in_ti")
        print(f"Value retrieved from Task Instance XCom: '{value}'")

    @task
    def return_value() -> str:
        return "Hello from return value!"
    
    @task
    def use_return_value(value: str) -> None:
        print(f"Value retrieved from return value: '{value}'")

    first_push = push_in_context()
    first_push >> get_from_context()

    first_push >> second_push_same_key() >> extract_multiple_values_same_key()

    push_in_ti() >> get_from_ti()
    push_dict() >> get_dict()

    value = return_value()
    use_return_value(value=value)

xcom_dag()
