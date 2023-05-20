from airflow import DAG
from airflow.utils import timezone
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def _world():
    print("world")

with DAG(
    dag_id="8_30_tuesdays",
    schedule="30 8 * * 2",
    start_date=timezone.datetime(2023, 5, 1),
    tags=["DEB","Skooldio","2023"],
    catchup=False,
):

    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'hello'",
    )

    world = PythonOperator(
        task_id="world",
        python_callable=_world,
    )

    hello >> world
