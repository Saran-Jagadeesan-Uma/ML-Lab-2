from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from collections import Counter
from src.news_utils import fetch_news, clean_text, count_words, save_report

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2025, 10, 5),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'news_wordcount_pipeline',
    default_args=default_args,
    description='Fetch news, count words, and save report',
    schedule_interval=None,
    catchup=False
)

# Task 1: Fetch news
def fetch_news_task_callable(**context):
    text = fetch_news()
    return text

task_fetch = PythonOperator(
    task_id='fetch_news_task',
    python_callable=fetch_news_task_callable,
    provide_context=True,
    dag=dag
)

# Task 2: Clean text
def clean_text_task_callable(**context):
    ti = context['ti']
    raw_text = ti.xcom_pull(task_ids='fetch_news_task')
    cleaned = clean_text(raw_text)
    return cleaned

task_clean = PythonOperator(
    task_id='clean_text_task',
    python_callable=clean_text_task_callable,
    provide_context=True,
    dag=dag
)

# Task 3: Count words
def count_words_task_callable(**context):
    ti = context['ti']
    cleaned_text = ti.xcom_pull(task_ids='clean_text_task')
    word_counts = count_words(cleaned_text)
    return dict(word_counts)

task_count = PythonOperator(
    task_id='count_words_task',
    python_callable=count_words_task_callable,
    provide_context=True,
    dag=dag
)

# Task 4: Save report
def save_report_task_callable(**context):
    ti = context['ti']
    word_counts_dict = ti.xcom_pull(task_ids='count_words_task')
    word_counts = Counter(word_counts_dict)
    save_report(word_counts)

task_save = PythonOperator(
    task_id='save_report_task',
    python_callable=save_report_task_callable,
    provide_context=True,
    dag=dag
)

task_fetch >> task_clean >> task_count >> task_save

if __name__ == "__main__":
    dag.cli()
