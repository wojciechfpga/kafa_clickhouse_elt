from datetime import datetime
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.http_sensor import HttpSensor

from utils.config import DEFAULT_ARGS 
from utils.dag_helpers import get_most_recent_init_dag_success

with DAG(
    dag_id="dbt",
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    description="Triggers dbt debug, run, and report creation"
) as dag:

    wait_for_init = ExternalTaskSensor(
        task_id="wait_for_init",
        external_dag_id="init",
        external_task_id=None, 
        mode="poke",
        poke_interval=60,
        timeout=60 * 60 * 24,
        execution_date_fn=get_most_recent_init_dag_success,
    )

    dbt_debug = HttpSensor(
        task_id="dbt_debug",
        http_conn_id="dbt_api",
        endpoint="/dbt-debug",
        poke_interval=10,
        timeout=600,
    )

    dbt_run = HttpSensor(
        task_id="dbt_run",
        http_conn_id="dbt_api",
        endpoint="/dbt-run",
        poke_interval=10,
        timeout=600,
        method='POST',
    )

    create_report = HttpSensor(
        task_id="create_report",
        http_conn_id="raport-creator",
        endpoint="/create_raport",
        poke_interval=10,
        timeout=600,
        method='POST',
    )

    wait_for_init >> dbt_debug >> dbt_run >> create_report