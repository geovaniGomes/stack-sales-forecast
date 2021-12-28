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

dag = DAG('05_etl_order_items_merge', 
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

    client.fget_object('processing', 'olist_geolocation.parquet', 'temp_.parquet')
    df_geolocalization = pd.read_parquet('temp_.parquet')

    client.fget_object('processing', 'olist_products.parquet', 'temp_.parquet')
    df_products = pd.read_parquet('temp_.parquet')
    
    #persiste os arquivos na área de Staging.
    df_order.to_csv("/tmp/orders.csv",index=False)
    df_order_items.to_csv("/tmp/orders_items.csv",index=False)
    df_customers.to_csv("/tmp/customers.csv",index=False)
    df_geolocalization.to_csv("/tmp/geolocalization.csv", index=False)
    df_products.to_csv("/tmp/products.csv", index=False)


def transform():
    df_one = pd.read_csv("/tmp/orders.csv")
    df_two = pd.read_csv("/tmp/orders_items.csv")
    df_three = pd.read_csv("/tmp/customers.csv")
    df_for = pd.read_csv("/tmp/geolocalization.csv")
    df_five = pd.read_csv("/tmp/products.csv")

    df_order = df_one.copy()
    df_order_items = df_two.copy()
    df_customers = df_three.copy()
    df_geolocalization = df_for.copy()
    df_products = df_five.copy()


    # Inner join entre data set order e order_intems
    df_order_items_merge = pd.merge(df_order,df_order_items, on="order_id")

    df_geo_cordenates = df_geolocalization[['geolocation_zip_code_prefix',
                                          'geolocation_lat',
                                          'geolocation_lng']]
    
    df_clients_geographical = pd.merge(df_customers,df_geo_cordenates, right_on='geolocation_zip_code_prefix', left_on='customer_zip_code_prefix')
    
    df_clients_geographical.drop(columns=[
        'customer_zip_code_prefix',
        'geolocation_zip_code_prefix'
        ],axis=1, inplace=True)

    df = pd.merge(df_order_items_merge, df_clients_geographical, on='customer_id')
    df_products_filter = df_products[['product_id',
                                    'product_category_name',
                                    'product_photos_qty',
                                    'product_weight_g',
                                    'product_length_cm',
                                    'product_height_cm',
                                    'product_width_cm']]
    
    list_to_category  = ['order_id',
                    'customer_id', 
                    'order_purchase_timestamp', 
                    'order_approved_at', 
                    'order_delivered_carrier_date', 
                    'order_delivered_customer_date',
                    'order_estimated_delivery_date',
                    'order_purchase_month_name',
                    'delivered_customer_month_name',
                    'order_purchase_time_day',
                    'order_delivered_customer_time_day',
                    'product_id',
                    'shipping_limit_date',
                    'customer_city']
    

    for column in list_to_category:
        df[column] = df[column].astype('category')
    
    df_part_1 = df[:8419172]
    df_part_2 = df[8419171:]

    del df_order
    del df_order_items
    del df_geolocalization
    del df_customers
    del df_geo_cordenates
    del df_order_items_merge
    del df_clients_geographical


    df_finaly_1 = pd.merge(df_part_1, df_products_filter, on='product_id')
    df_finaly_2 = pd.merge(df_part_2, df_products_filter, on='product_id')

    df_finaly_1.to_csv("/tmp/order_items_merge_part_1.csv", index=False)
    df_finaly_2.to_csv("/tmp/order_items_merge_part_2.csv", index=False)

    del df_finaly_1
    del df_finaly_2

def load_pt_1():
    df_1 = pd.read_csv("/tmp/order_items_merge_part_1.csv")

    df_1.to_parquet(
        "/tmp/order_items_merge_part_1.parquet"
        ,index=False
    )
        #carrega os dados para o Data Lake.
    client.fput_object(
        "processing",
        "order_items_merge_part_1.parquet",
        "/tmp/order_items_merge_part_1.parquet"
    )
   

def load_pt_2():
    df_2 = pd.read_csv("/tmp/order_items_merge_part_2.csv")
    df_2.to_parquet(
        "/tmp/order_items_merge_part_2.parquet"
        ,index=False
    )

    client.fput_object(
        "processing",
        "order_items_merge_part_2.parquet",
        "/tmp/order_items_merge_part_2.parquet"
    )


extract_task = PythonOperator(
    task_id='extract_olist_order_items_merge_data_lake',
    provide_context=True,
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform_olist_order_items_merge_data_lake",
    provide_context=True,
    python_callable=transform,
    dag=dag
)

load_task_one = PythonOperator(
    task_id='load_olist_order_items_merge_pt_1_data_lake',
    provide_context=True,
    python_callable=load_pt_1,
    dag=dag
)

load_task_two = PythonOperator(
    task_id='load_olist_order_items_merge_pt_2_data_lake',
    provide_context=True,
    python_callable=load_pt_2,
    dag=dag
)

clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/*.csv;rm -f /tmp/*.json;rm -f /tmp/*.parquet;",
    dag=dag
)

extract_task >> transform_task >> load_task_one >> load_task_two >> clean_task
