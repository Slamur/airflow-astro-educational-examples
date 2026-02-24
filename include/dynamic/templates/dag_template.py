from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2026, 2, 24),
    schedule="SCHEDULE_INTERVAL_HOLDER",
    tags=["dynamic_dag", "example"],
    catchup=False,
)
def process_DAG_ID_HOLDER():

    @task
    def extract_data(file_name: str):
        return f"Extracted data from '{file_name}'"
    
    @task
    def transform_data(extracted_data: str):
        return f"Transformed data from '{extracted_data}'"

    @task
    def load_data(transformed_data: str, location: str):
        print(f"'{transformed_data}' loaded to location '{location}'")

    extracted_data = extract_data("INPUT_HOLDER")
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data, location="LOCATION_HOLDER")

process_DAG_ID_HOLDER()