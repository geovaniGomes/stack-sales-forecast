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

dag = DAG('04_etl_reviews', 
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
        )

client = ConectionMinio().instance()


def extract():
    
    #extrai os dados a partir do Data Lake.
    obj = client.get_object(
                "landing",
                "olist_order_reviews_dataset.csv",
    )
    data = obj.read()
    data = StringIO(str(data, 'utf-8')) 
    df_ = pd.read_csv(data)
    
    #persiste os arquivos na área de Staging.
    df_.to_csv("/tmp/reviews.csv",
                index=False)


def transform():
    df = pd.read_csv("/tmp/reviews.csv")
    df.drop_duplicates(subset=['review_id'], inplace=True)
    df.drop(columns=['review_comment_title'], axis=1, inplace=True)
    df.to_csv("/tmp/reviews.csv", index=False)

def load():
    df = pd.read_csv("/tmp/reviews.csv")

     #converte os dados para o formato parquet.
    df.to_parquet(
        "/tmp/reviews.parquet"
        ,index=False
    )

    #carrega os dados para o Data Lake.
    client.fput_object(
        "processing",
        "olist_reviews.parquet",
        "/tmp/reviews.parquet"
    )


extract_task = PythonOperator(
    task_id='extract_olist_reviews_data_lake',
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
    task_id='load_olist_reviews_data_lake',
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
