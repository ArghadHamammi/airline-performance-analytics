from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='airline_data_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    # المهمة الأولى: جمع البيانات
    ingest_data_task = BashOperator(
        task_id='ingest_data',
        bash_command='python /opt/airflow/scripts/get_flight_data.py'
    )

    # المهمة الثانية: معالجة البيانات
    transform_data_task = BashOperator(
        task_id='transform_data',
        bash_command='python /opt/airflow/scripts/transform_data.py'
    )

    # المهمة الثالثة: تحميل البيانات إلى قاعدة البيانات
    load_to_db_task = BashOperator(
        task_id='load_to_db',
        bash_command='python /opt/airflow/scripts/load_to_db.py'
    )

    # تحديد ترتيب تنفيذ المهام
    # (استخدمنا علامة >> للتعبير عن التبعية)
    ingest_data_task >> transform_data_task >> load_to_db_task