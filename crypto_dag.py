from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pivot_data import pivot_crypto_data
from extract import fetch_crypto_data
from time_series_analysis import analyze_and_forecast_crypto

default_args={
        'owner': 'Gilbert',
        'depend_on_past':False,
        'email_on_failure':False,
        'email_on_retry':False,
        'retries':2,
        'retry_delay':timedelta(minutes=3),
 
        }

with DAG(
        'crypto_prices_dag',
        default_args=default_args,
        schedule='@hourly',
        start_date=datetime(2025,6,13),
        catchup=False,


        )as dag:


    pivot_data_task=PythonOperator(
            task_id='pivot_crypto_data',
            python_callable=pivot_crypto_data,


            )

    fetch_data_task=PythonOperator(
            task_id='fetch_crypto_data',
            python_callable=fetch_crypto_data,

            )

    analyze_and_predict_task=PythonOperator(
            task_id='analyze_and_forecast_crypto',
            python_callable=analyze_and_forecast_crypto,


            )


    fetch_data_task >> pivot_data_task >> analyze_and_predict_task



