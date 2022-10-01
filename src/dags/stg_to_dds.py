import pendulum
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)
pg_conn_id = 'PG_WAREHOUSE_CONNECTION'


def run_directory_command(directory, source_script):
    with open(source_script) as f:
        sql_command = f.read()
    sql_command = sql_command.replace("$directory", directory)
    pg_hook = PostgresHook(pg_conn_id)
    pg_hook.run(sql_command)

def run_facts_command(source_script):
    with open(source_script) as f:
        sql_command = f.read()
    pg_hook = PostgresHook(pg_conn_id)
    pg_hook.run(sql_command)


dag = DAG(
    dag_id="stg_to_dds",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 2, 10)
)

start = EmptyOperator(task_id="start", dag=dag)


directories = ['event_id', 'event_timestamp', 'event_type', 'ip_address', 'page_url', 'page_url_path']
truncate_directories = [
    PythonOperator(
        task_id=f'truncate_{directory}',
        python_callable=run_directory_command,
        op_kwargs={
            "directory": directory,
            "source_script": "/lessons/sql/truncate_command.sql"
        },
        dag=dag
    )

    for directory in directories
]

truncate_facts = PythonOperator(
    task_id="truncate_facts",
    python_callable=run_facts_command,
    op_kwargs={
        "source_script": "/lessons/sql/truncate_facts.sql"
    }
)

connector = EmptyOperator(task_id="connector_between_groups", dag=dag)

ingest_directories = [
    PythonOperator(
        task_id=f'load_{directory}',
        python_callable=run_directory_command,
        op_kwargs={
            "directory": directory,
            "source_script": f"/lessons/sql/ingest_command.sql"
        },
        dag=dag
    )

    for directory in directories
]

ingest_facts = PythonOperator(
    task_id="ingest_facts",
    python_callable=run_facts_command,
    op_kwargs={
        "source_script": "/lessons/sql/ingest_facts.sql"
    }
)

end = EmptyOperator(task_id="end", dag=dag)

start >> truncate_facts >> truncate_directories >> connector >> ingest_directories >> ingest_facts >> end

