from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2026, 2, 18),
    schedule="@daily",
    description="DAG to check file creation process (bash example)",
    tags=["example", "bash"],
)
def bash_dag():

    tmp_file_path = "/tmp/test_file.txt"

    @task.bash
    def create_file() -> None:
        return f"echo 'This is a test file.' > {tmp_file_path}"

    @task.bash
    def check_file_exists() -> None:
        return f"test -f {tmp_file_path}"

    @task
    def read_file() -> str:
        with open(tmp_file_path, "r") as file:
            content = file.read()
        return content

    create_file() >> check_file_exists() >> read_file()

bash_dag()