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

dag = DAG('07_etl_olist', 
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
        )

client = ConectionMinio().instance()


def extract():
    
    #extrai os dados a partir do Data Lake.
    client.fget_object('processing', 'olist_orders.parquet', 'temp_.parquet')
    df_order = pd.read_parquet("temp_.parquet")

    client.fget_object('processing', 'olist_order_items.parquet', 'temp_.parquet')
    df_order_items = pd.read_parquet("temp_.parquet")

    client.fget_object('processing', 'olist_customers.parquet', 'temp_.parquet')
    df_customers = pd.read_parquet('temp_.parquet')

    client.fget_object('processing', 'olist_products.parquet', 'temp_.parquet')
    df_products = pd.read_parquet('temp_.parquet')

    client.fget_object('processing', 'olist_reviews.parquet', 'temp_.parquet')
    df_reviews = pd.read_parquet('temp_.parquet')

    client.fget_object('processing', 'olist_sellers.parquet', 'temp_.parquet')
    df_sellers = pd.read_parquet('temp_.parquet')

    
    #persiste os arquivos na área de Staging.
    df_order.to_csv("/tmp/orders.csv",index=False)
    df_order_items.to_csv("/tmp/orders_items.csv",index=False)
    df_customers.to_csv("/tmp/customers.csv",index=False)
    df_products.to_csv("/tmp/products.csv", index=False)
    df_reviews.to_csv("/tmp/reviews.csv", index=False)
    df_sellers.to_csv("/tmp/sellers.csv", index=False)

    del df_order
    del df_order_items
    del df_customers
    del df_products
    del df_reviews
    del df_sellers


def transform():
    df_one = pd.read_csv("/tmp/orders.csv")
    df_two = pd.read_csv("/tmp/orders_items.csv")
    df_three = pd.read_csv("/tmp/customers.csv")
    df_for = pd.read_csv("/tmp/products.csv")
    df_five = pd.read_csv("/tmp/reviews.csv")
    df_seven = pd.read_csv("/tmp/sellers.csv")

    df_order = df_one.copy()
    df_order_items = df_two.copy()
    df_customers = df_three.copy()
    df_products = df_for.copy()
    df_reviews = df_five.copy()
    df_sellers = df_seven.copy()

    del df_one
    del df_two
    del df_three
    del df_for
    del df_five
    del df_seven


    # Inner join entre data set order e order_intems

    olist = df_order.merge(df_order_items, on='order_id', how='inner')
    olist = olist.merge(df_products, on="product_id", how='inner')
    olist = olist.merge(df_customers, on="customer_id", how='inner')  
    olist = olist.merge(df_reviews, on="order_id", how='inner')

    print(olist.columns)
    olist = olist.merge(df_sellers, on="seller_id", how='inner')

    olist.to_csv("/tmp/olist_data_set.csv", index=False)

def load():
    df = pd.read_csv("/tmp/olist_data_set.csv")

    df.to_parquet(
        "/tmp/olist_data_set.parquet"
        ,index=False
    )
        #carrega os dados para o Data Lake.
    
    del df

    client.fput_object(
        "processing",
        "olist_data_set.parquet",
        "/tmp/olist_data_set.parquet"
    )
   

extract_task = PythonOperator(
    task_id='extract_olist_data_lake',
    provide_context=True,
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform_olist_data_lake",
    provide_context=True,
    python_callable=transform,
    dag=dag
)

load_task_one = PythonOperator(
    task_id='load_olist_order_data_lake',
    provide_context=True,
    python_callable=load,
    dag=dag
)

clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/*.csv;rm -f /tmp/*.json;rm -f /tmp/*.parquet;",
    dag=dag
)

extract_task >> transform_task >> load_task_one >> clean_task
