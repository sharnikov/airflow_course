from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

pushTimeTaskId = Variable.get("pushTimeTaskId") or "pushTime"
placeToStoreResultFile = Variable.get("placeToStoreResultFile") or "/usr/local/airflow/dags"


def buildSubDag(dag_id, filePath, dagIdToWait):

    with DAG(
        dag_id=dag_id,
        schedule_interval=None,
        start_date=datetime(2018, 11, 11)
    ) as dag:

        def takeTime(**context):
            time = context['ti'].xcom_pull(task_ids=pushTimeTaskId)
            print(f'The time was taken {time}')
            return time
        
        externalTaskSensor = ExternalTaskSensor(
            task_id="wait_for_other_dag",
            external_dag_id=dagIdToWait,
            execution_delta=timedelta(minutes=0),
            external_task_id=None,
        )

        printOperator = PythonOperator(
            task_id="print_time",
            python_callable=takeTime,
            provide_context=True
        )
        
        removeFileOp = BashOperator(
            task_id="remove_file",
            bash_command="rm {}".format(filePath)
        )
        
        finishedDateOp = BashOperator(
            task_id="_create_finish_date_file",
            bash_command="touch {}/finished_{}".format(placeToStoreResultFile, "{{ ds }}")
        )
        
        externalTaskSensor >> printOperator >> removeFileOp >> finishedDateOp
        return dag
