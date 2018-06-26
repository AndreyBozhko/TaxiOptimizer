import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


schedule_interval = timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - schedule_interval,
    'email': ['andybozhko@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2019, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': callback_function,
    # 'on_success_callback': callback_function,
    # 'on_retry_callback': callback_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    'batch_scheduler',
    default_args=default_args,
    description='DAG for the Spark Batch Job',
    schedule_interval=schedule_interval)


task = BashOperator(
    task_id='run_batch_job',
    bash_command='cd /home/ubuntu/TaxiOptimizer/ ; ./spark-run.sh --batch',
    dag=dag)


task.doc_md = """\
#### Task Documentation
Spark Batch Job is scheduled to start every day
"""

dag.doc_md = __doc__
