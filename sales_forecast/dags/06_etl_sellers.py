import ast
from datetime import datetime
from io import StringIO
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from pandas.io.parsers import read_csv
from helpers.settings import ConectionMinio

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 13),
}

dag = DAG('06_etl_sellers', 
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
        )

client = ConectionMinio().instance()


def extract():
    
    #extrai os dados a partir do Data Lake.
    obj = client.get_object(
                "landing",
                "olist_sellers_dataset.csv",
    )
    data = obj.read()
    data = StringIO(str(data, 'utf-8')) 
    df_ = pd.read_csv(data)
    
    #persiste os arquivos na área de Staging.
    df_.to_csv("/tmp/sellers.csv",
                index=False)


def transform():
    df = pd.read_csv("/tmp/sellers.csv")
    df.drop(columns=[
        'seller_zip_code_prefix',
        'seller_city',
        'seller_state'], axis=1, inplace=True)

    df.to_csv("/tmp/sellers.csv", index=False)

def load():
    df = pd.read_csv("/tmp/sellers.csv")

     #converte os dados para o formato parquet.
    df.to_parquet(
        "/tmp/sellers.parquet"
        ,index=False
    )

    #carrega os dados para o Data Lake.
    client.fput_object(
        "processing",
        "olist_sellers.parquet",
        "/tmp/sellers.parquet"
    )


extract_task = PythonOperator(
    task_id='extract_olist_sellers_data_lake',
    provide_context=True,
    python_callable=extract,
    dag=dag
)
transform_task = PythonOperator(
    task_id='transform_data',
    provide_context=True,
    python_callable=transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_olist_sellers_data_lake',
    provide_context=True,
    python_callable=load,
    dag=dag
)

clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/*.csv;rm -f /tmp/*.json;rm -f /tmp/*.parquet;",
    dag=dag
)

extract_task >> transform_task >> load_task >> clean_task
