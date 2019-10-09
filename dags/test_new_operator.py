from datetime import datetime

from airflow import DAG

from PostgreSQLCountRows import PostgreSQLCountRows

dag = DAG(
    dag_id="test_own_operator_dag",
    schedule_interval=None,
    start_date=datetime(2018, 11, 11)
)

postgreCountOperator = PostgreSQLCountRows(task_id="count_operator",
                                           schema="public",
                                           table_name="dag1TableName",
                                           dag=dag)

globals()["postgreCountOperator"] = dag

postgreCountOperator
