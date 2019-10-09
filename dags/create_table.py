from datetime import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

import random

dag = DAG(
    dag_id="PostgresDag",
    schedule_interval=None,
    start_date=datetime(2018, 11, 11)
)

tableName = "testTable"

createTableOp = PostgresOperator(
    task_id="create_table",
    sql=f"CREATE TABLE {tableName}(id integer NOT NULL, userName CHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL "
        f"DEFAULT NOW());",
    dag=dag
)

insertRowOp = PostgresOperator(
    task_id="kinda_insert_new_row",
    sql="INSERT INTO {}(id, userName) VALUES ({}, {});".format(tableName, random.randint(0, 10000000), "user"),
    trigger_rule="all_done",
    provide_context=True,
    dag=dag
)

globals()['create_table'] = dag

createTableOp >> insertRowOp
