import pendulum
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)
pg_conn_id = 'PG_WAREHOUSE_CONNECTION'


def run_sql_command(source_script):
    with open(source_script) as f:
        sql_command = f.read()
    pg_hook = PostgresHook(pg_conn_id)
    pg_hook.run(sql_command)



with DAG(
    dag_id="dds_to_cdm",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 2, 10)
) as dag:
    start = EmptyOperator(task_id="start")
    run_task1 = PythonOperator(
        task_id="task1",
        python_callable=run_sql_command,
        op_kwargs = {
            "source_script": "/lessons/sql/task1.sql"
        }
    )
    run_task2 = PythonOperator(
        task_id="task2",
        python_callable=run_sql_command,
        op_kwargs = {
            "source_script": "/lessons/sql/task2.sql"
        }
    )
    end = EmptyOperator(task_id="end")
    start >> run_task1 >> run_task2 >> end