from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

fetch_data = BashOperator(
    task_id="fetch_data",
    bash_command="python /tests/predict_test.py",
)
#chech ghg