import zipfile
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import io
import pendulum
import logging
import pandas as pd
import json
import hashlib
from os import listdir
from os.path import isfile, join, splitext
from sqlalchemy import create_engine


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


log = logging.getLogger(__name__)
engine = create_engine('postgresql+psycopg2://jovyan:jovyan@localhost/de')


def download_unpack_zips(url, target_folder):
    """
    Get files contained in zip from url
    """
    file_links = []
    r = requests.get(url)

    if r.status_code != 200:
        log.warn(f"Request failed. Reason:\n {r.content}")
        return

    soup = BeautifulSoup(r.text, features="html.parser")
    links = soup.findAll('a', href=True)
    for link in links:
        if link['href'].endswith('.zip'):
            file_links.append(url + link['href'])

    
    Path(target_folder).mkdir(parents=True, exist_ok=True)
    for link in file_links:
        r = requests.get(link)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(target_folder)

def load_file_to_stg(file_name, target_table):
    """
    Load single json file named "file_name" to staging
    """
    d = None  
    data = None  
    with open(file_name) as f:  
        data = f.read()  
        d = json.loads(data)
    to_db = []

    for obj in d:
        m = {}
        json_str = json.dumps(obj)
        m['hash'] = hashlib.md5(json_str.encode('utf-8')).hexdigest()
        m['object_value'] = obj
        m['source'] = 'logs'
        to_db.append(m)
    df = pd.DataFrame.from_dict(to_db)
    df['object_value'] = df['object_value'].apply(json.dumps)
    df.to_sql(con=engine, name=target_table, schema='team_1_stg',if_exists='replace')

def create_tasks_to_load(source_folder, dag):
    """
    Create tasks to upload all the files
    """
    load_tasks = []
    for i in listdir(source_folder):
        if isfile(join(source_folder, i)):
            file_name_wo_ext = splitext(i)[0]
            load_tasks.append(
                PythonOperator(
                    task_id = f'load_{file_name_wo_ext}',
                    python_callable=load_file_to_stg,
                    op_kwargs={
                        "file_name": f'{source_folder}/{i}',
                        "target_table": 'logs'
                    },
                    dag = dag
                )
            )
    return load_tasks
        

dag =  DAG(
    dag_id = "load_to_stg",
    schedule_interval = "@daily",
    start_date = pendulum.datetime(2022, 1, 1)
)

start = EmptyOperator(task_id="start", dag=dag)

download_files = PythonOperator(
    task_id = 'download_files',
        python_callable=download_unpack_zips,
        op_kwargs={
            "url": "https://data.ijklmn.xyz/events/",
            "target_folder": "/data"
        },
        dag = dag
)



upload_files = create_tasks_to_load("/data", dag)

end = EmptyOperator(task_id="end", dag=dag)
    
start >> download_files >> upload_files >> end