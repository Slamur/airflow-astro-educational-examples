import os
import shutil
import fileinput
import json

TEMPLATE_PATH = "include/dynamic/templates/dag_template.py"

DAGS_DATA_PATH = "include/dynamic/data/"
DAGS_DATA_EXTENSION = ".json"

OUTPUT_DIR = "dags/dynamic/"
GENERATED_DAG_FILE_PATTERN = "process_{}_dag.py"

def create_dag_file(dag_id: str):
    output_file_name = GENERATED_DAG_FILE_PATTERN.format(dag_id)
    output_file_path = os.path.join(OUTPUT_DIR, output_file_name)

    shutil.copy(TEMPLATE_PATH, output_file_path)

    return output_file_path


def generate_dag(config: dict):
    dag_id = config["dag_id"]
    dag_file_path = create_dag_file(dag_id)

    # Replace placeholders in the copied file with actual values from config
    for line in fileinput.FileInput(dag_file_path, inplace=True):
        line = line.replace("DAG_ID_HOLDER", dag_id)
        line = line.replace("SCHEDULE_INTERVAL_HOLDER", config["schedule_interval"])
        line = line.replace("INPUT_HOLDER", config["input"])
        line = line.replace("LOCATION_HOLDER", config["location"])
        print(line, end='')


def generate_dags():
    for file_name in os.listdir(DAGS_DATA_PATH):
        if not file_name.endswith(DAGS_DATA_EXTENSION):
            continue

        config = json.load(open(os.path.join(DAGS_DATA_PATH, file_name)))
        generate_dag(config)


generate_dags()