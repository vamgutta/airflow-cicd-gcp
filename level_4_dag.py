# import all modules
import airflow
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


args = {
    "owner" : "developer",
    "start_date" : datetime(2023,10,13),
    "retries" : 1,
    "retry_delay" : timedelta(minutes=5)
}

# define the DAG
with DAG(
    "PARENT_DAG_COMPOSER",
    schedule_interval = '30 7 * * *',
    default_args = args
) as dag:
    
    task_1 = TriggerDagRunOperator(
        task_id= "trigger_level_1",
        trigger_dag_id="LEVEL_1_DAG_COMPOSER"
    )

    task_2 = TriggerDagRunOperator(
        task_id= "trigger_level_2",
        trigger_dag_id="LEVEL_2_DAG_COMPOSER"
    )

#depe
(task_1,task_2)
