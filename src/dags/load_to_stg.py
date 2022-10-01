import zipfile
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import io

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

def download_zips(url, target_folder):
    """
    get list of href links ending on .zip from url
    """
    file_links = []
    r = requests.get(url)
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

with DAG(
    dag_id = "load_to_stg",
    schedule_interval = None
) as dag:
    start = EmptyOperator("start")
    download_files = PythonOperator(
        task_id = 'download_files',
        python_callable=download_zips,
        op_kwargs={
            "url": "https://data.ijklmn.xyz/events/",
            "target_folder": "/data"
        }
    )
    start >> download_files