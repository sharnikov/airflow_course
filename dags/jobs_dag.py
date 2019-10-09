from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook

from airflow.models import Variable

import random

config = {
    "dag_id_1": {"schedule_interval": None, "start_date": datetime(2018, 11, 11), "tableName": "dag1TableName"},
    "dag_id_2": {"schedule_interval": None, "start_date": datetime(2018, 11, 11), "tableName": "dag2TableName"},
    "dag_id_3": {"schedule_interval": None, "start_date": datetime(2018, 11, 11), "tableName": "dag3TableName"}
}

skip_creation_task_id = "skip_table_creation"
push_current_user = "push_current_user"
create_table_task_id = "create_table"

pushTimeTaskId = Variable.get("pushTimeTaskId") or "pushTime"


def printLog(dagId):
    print("{} start processing tables in database".format(dagId))


def check_table_exist(get_all_tables_query,
                      table_name,
                      create_table_task_id,
                      skip_creation_task_id):
    hook = PostgresHook()
    lower_table_name = table_name.lower()

    allTables = hook.get_records(sql=get_all_tables_query)
    for result in allTables:
        if lower_table_name in result:
            return skip_creation_task_id

    return create_table_task_id


for dagKey in config:
    dagInfo = config[dagKey]
    with DAG(
            dag_id=dagKey,
            schedule_interval=dagInfo["schedule_interval"],
            start_date=dagInfo["start_date"]
    ) as dag:
        tableName = dagInfo["tableName"]

        skipTableCreationOp = DummyOperator(task_id=skip_creation_task_id)

        pushUserName = BashOperator(
            task_id=push_current_user,
            bash_command='echo JohnDeGoes',
            xcom_push=True
        )

        createTableBranchOp = BranchPythonOperator(
            task_id='create_table_branch',
            python_callable=check_table_exist,
            op_args=[
                "SELECT * FROM pg_tables;",
                tableName,
                create_table_task_id,
                skip_creation_task_id
            ]
        )

        createTableOp = PostgresOperator(
            task_id=create_table_task_id,
            sql=f"CREATE TABLE {tableName}(id integer NOT NULL, userName CHAR (50) NOT NULL, timestamp TIMESTAMP NOT "
                f"NULL DEFAULT NOW()); "
        )

        printLogOp = PythonOperator(
            task_id="print_log",
            python_callable=printLog,
            op_args=[dagKey]
        )


        def insertRow(tableName, **context):
            userName = context['ti'].xcom_pull(key=None, task_ids=push_current_user)
            hook = PostgresHook()
            hook.insert_rows(table=tableName,
                             rows=[(random.randint(0, 10000000), userName)],
                             target_fields=["id", "userName"])


        insertRowOp = PythonOperator(
            task_id="insert_new_row",
            python_callable=insertRow,
            op_args=[tableName],
            trigger_rule="all_done",
            provide_context=True
        )


        def printRowsAmount(table):
            hook = PostgresHook()
            amount = hook.get_records(sql=f"SELECT COUNT(*) FROM {table};")
            print(f"Amount of rows = {amount}")
            return amount


        queryTable = PythonOperator(
            task_id="select_rows_amount",
            python_callable=printRowsAmount,
            op_args=[tableName]
        )

        def pushTime():
            return "{{ run_id }}"

        pushTimeOperator = PythonOperator(
            task_id=pushTimeTaskId,
            python_callable=pushTime
        )

        globals()[dagKey] = dag

        printLogOp >> pushUserName >> createTableBranchOp >> [createTableOp,
                                                              skipTableCreationOp] >> insertRowOp
        insertRowOp >> queryTable >> pushTimeOperator
