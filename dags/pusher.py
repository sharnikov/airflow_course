from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

with DAG(
    dag_id="pushPull",
    schedule_interval=None,
    start_date=datetime(2018, 11, 11)
) as dag:

    def pushTime():
        return "{{ ts }}"

    push = PythonOperator(
        task_id="push",
        python_callable=pushTime
    )

    def takeTime(**context):
        time=context['ti'].xcom_pull(task_ids='push')
        print(f'The time was taken {time}')
        return time

    pull = PythonOperator(
                task_id="pull",
                python_callable=takeTime,
                provide_context=True
            )

    push >> pull

    globals()["pushPull"] = dag
