from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email


args = {
    'owner': 'airflow',
    'email': ['patrice.nzi@orange.com']
}

with DAG(
    dag_id='etl_detailed_graph_telco',
    default_args=args,
    schedule_interval='0 19 * * 1',
    start_date=days_ago(7),
    dagrun_timeout=timedelta(minutes=60),
    tags=['orange-bank', 'telco-graph'],
    on_failure_callback=send_email,
    params={"clients": "/home/davidtia/output/AllCustomers_OBA_CI_old_last.csv", "destination": "/home/davidtia/incoming/recurrence/graph_telco/", "auto": "true", "start_date": "20220131", "end_date": "20220206"},
) as dag:

    run_this_last = DummyOperator(task_id='run_this_last',)

    execute_etl_bash = 'spark-submit --conf spark.pyspark.python=/home/patrice.nzi/backup/oba/obavenv/bin/python --master local[*] --packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 --jars /home/patrice.nzi/backup/oba/jars/terajdbc4.jar /home/patrice.nzi/backup/oba/airflow_prod/dags/backend/social_graph/extract_and_deliver_telco_graph_indicators.py --oba_clients {{ params.clients }} --destination {{ params.destination }} -start-date {{ params.start_date }} -end-date {{ params.end_date }} --auto {{ params.auto }}'

    extract_and_deliver = BashOperator(task_id='extract_and_deliver', bash_command=execute_etl_bash, dag=dag,)

    extract_and_deliver >> run_this_last
