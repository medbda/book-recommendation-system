from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from review_scraper import scrape_top_100_reviews
from review_cleaning import tokenize_reviews_per_book
import pandas as pd


default_args = {
    'owner': 'vaibhavi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}



with DAG(
    dag_id='book_review_pipeline_test',
    default_args=default_args,
    start_date=datetime(2025, 8, 5),
    schedule_interval=None,  # Trigger manually
    catchup=False
) as dag:

    scrape_task = PythonOperator(
        task_id='scrape_reviews',
        python_callable=scrape_top_100_reviews
    )

    wordcount_task = PythonOperator(
        task_id='clean_reviews',
        python_callable=tokenize_reviews_per_book
    )

    scrape_task >> wordcount_task
