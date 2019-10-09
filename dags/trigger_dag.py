from datetime import datetime

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Variable

from trigger_sub_dag import buildSubDag

pathToFile = Variable.get("filePath") or "/usr/local/airflow/dags/az.txt"
dagIdToTrigger = Variable.get("dagToTrigger") or "dag_id_1"

dagId = "sensorDag"
subDagingTaskId = "subDaging"
subDagId = f"{dagId}.{subDagingTaskId}"

with DAG(
    dag_id=dagId,
    schedule_interval=None,
    start_date=datetime(2018, 11, 11)
) as dag:

    fileSensOp = FileSensor(
        task_id="file_sensor",
        filepath=pathToFile
    )

    triggerDagRunOp = TriggerDagRunOperator(
        task_id="trigger_dag_run",
        python_callable=lambda context, dagToTrigger: dagToTrigger,
        trigger_dag_id=dagIdToTrigger,
        execution_date="{{ ts }}"
    )

    subDagOp = SubDagOperator(
        subdag=buildSubDag(subDagId, pathToFile, dagIdToTrigger),
        task_id=subDagingTaskId
    )

    fileSensOp >> triggerDagRunOp >> subDagOp
