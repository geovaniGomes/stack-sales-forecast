import ast
from datetime import datetime
from io import StringIO
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from helpers.settings import ConectionMinio

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 13),
}

dag = DAG('03_etl_orders', 
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
        )

client = ConectionMinio().instance()


def extract():
    
    #extrai os dados a partir do Data Lake.
    obj = client.get_object(
                "landing",
                "olist_orders_dataset.csv",
    )
    data = obj.read()
    data = StringIO(str(data, 'utf-8')) 
    df_ = pd.read_csv(data)
    
    #persiste os arquivos na área de Staging.
    df_.to_csv("/tmp/orders.csv",
                index=False)

def transform():
    df_orders = pd.read_csv("/tmp/orders.csv")

    # filtro dataframe somente com pedidos entregues
    df_orders = df_orders.query("order_status=='delivered'")

    df = df_orders.copy()
    # lista de colunas para conversção em datetime
    timestamp_cols =['order_purchase_timestamp','order_approved_at', 'order_delivered_carrier_date',
       'order_delivered_customer_date', 'order_estimated_delivery_date']
    
    for col in timestamp_cols:
            df[col] = pd.to_datetime(df[col])

    # drop values NaN
    df.dropna(inplace=True)

    # Extraindo atributes de Mês e ano da data de compra 
    df['order_purchase_year'] = df['order_purchase_timestamp'].apply(lambda x: x.year)
    df['order_purchase_month'] = df['order_purchase_timestamp'].apply(lambda x: x.month)
    df['order_purchase_month_name'] = df['order_purchase_timestamp'].apply(lambda x: x.strftime('%b'))
    df['order_purchase_year_month'] = df['order_purchase_timestamp'].apply(lambda x: x.strftime('%Y%m'))
    df['order_purchase_date'] = df['order_purchase_timestamp'].apply(lambda x: x.strftime('%Y%m%d'))

    # Extraindo atributes de Mês e ano da data de entrega 
    df['delivered_customer_year'] = df['order_delivered_customer_date'].apply(lambda x: x.year)
    df['delivered_customer_month'] = df['order_delivered_customer_date'].apply(lambda x: x.month)
    df['delivered_customer_month_name'] = df['order_delivered_customer_date'].apply(lambda x: x.strftime('%b'))
    df['delivered_customer_year_month'] = df['order_delivered_customer_date'].apply(lambda x: x.strftime('%Y%m'))
    df['delivered_customer_date'] = df['order_delivered_customer_date'].apply(lambda x: x.strftime('%Y%m%d'))

    #Extraindo atributos para data de compra - hora e hora do dia
    df['order_purchase_hour'] = df['order_purchase_timestamp'].apply(lambda x: x.hour)
    hours_bins = [-0.1, 6, 12, 18, 23]
    hours_labels = ['Dawn', 'Morning', 'Afternoon', 'Night']
    df['order_purchase_time_day'] = pd.cut(df['order_purchase_hour'], hours_bins, labels=hours_labels)

     #Extraindo atributos para data de entrega da compra - hora e hora do dia
    df['order_delivered_customer_hour'] = df['order_delivered_customer_date'].apply(lambda x: x.hour)
    hours_bins = [-0.1, 6, 12, 18, 23]
    hours_labels = ['Dawn', 'Morning', 'Afternoon', 'Night']
    df['order_delivered_customer_time_day'] = pd.cut(df['order_delivered_customer_hour'], hours_bins, labels=hours_labels)

    df.to_csv("/tmp/orders.csv", index=False)

def load():
    df = pd.read_csv("/tmp/orders.csv")

     #converte os dados para o formato parquet.
    df.to_parquet(
        "/tmp/orders.parquet"
        ,index=False
    )

    #carrega os dados para o Data Lake.
    client.fput_object(
        "processing",
        "olist_orders.parquet",
        "/tmp/orders.parquet"
    )


extract_task = PythonOperator(
    task_id='extract_olist_orders_data_lake',
    provide_context=True,
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform_olist_orders_data_lake",
    provide_context=True,
    python_callable=transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_olist_orders_data_lake',
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
