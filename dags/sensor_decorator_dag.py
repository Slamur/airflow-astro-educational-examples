from airflow.sdk import dag, task
from pendulum import datetime
import requests

from airflow.sdk.bases.sensor import PokeReturnValue


@dag(
        start_date=datetime(2026, 2, 19), 
        schedule="@daily", 
        catchup=False,
        tags=["example", "sensor", "decorator"],
)
def sensor_decorator():

    @task.sensor(
            poke_interval=30, 
            timeout=300, 
            mode="poke"
    )
    def check_shibe_availability() -> PokeReturnValue:
        r = requests.get("http://shibe.online/api/shibes?count=1&urls=true")
        print(r.status_code)

        if r.status_code == 200:
            condition_met = True
            operator_return_value = r.json()
        else:
            condition_met = False
            operator_return_value = None
            print(f"Shibe URL returned the status code {r.status_code}")

        return PokeReturnValue(
            is_done=condition_met, 
            xcom_value=operator_return_value,
        )

    # print the URL to the picture
    @task
    def print_shibe_picture_url(url):
        print(url)

    print_shibe_picture_url(check_shibe_availability())


sensor_decorator()