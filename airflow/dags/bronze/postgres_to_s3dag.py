from datetime import timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.dates import days_ago

table_name = '<>'
postgresurl = Variable.get('postgres_url')
username = Variable.get('postgres_username')
password = Variable.get('postgres_password')
s3_bucket = Variable.get('s3_bucket')
cluster_id = '<>'


default_args = {
    "owner": "Tomiwa",
    "start_date": days_ago(1),
    "depends_on_past": False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=4)
}

job_name = 'exisitng_emr_write_to_s3'
description = 'emr write to s3'
schedule_interval = '1 6 * * MON-SAT'
tags = ["spark", "etl", "emr"]

# JOB_FLOW_OVERRIDES = Variable.get("JOB_FLOW_OVERRIDES")
SPARK_STEPS = [
 {
        'Name': 'Run PySpark ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--master', 'yarn', '--packages', 'org.postgresql:postgresql:42.6.0',
                      f'{s3_bucket}/postgres_to_s3_full_load.py', '--postgresurl', postgresurl, '--table_name', table_name,
                    '--database_username', username, '--database_password', password, '--s3_output_path',  s3_bucket]
            }
        }
]

dag = DAG(
    dag_id=job_name,
    default_args=default_args,
    description=description,
    schedule_interval=schedule_interval,
    start_date=days_ago(1),
    catchup=False,
    tags=tags
)

# def _get_cluster_id(ti):
#     cluster_id = ti.xcom_pull(task_ids="get_cluster_id")
#     if cluster_id == "No cluster":
#         return "create_job_flow"
#     return "step_adder"


check = EmptyOperator(task_id= "check", dag=dag)

# create_job_flow = EmrCreateJobFlowOperator(
#     task_id="create_job_flow",
#     job_flow_overrides=JOB_FLOW_OVERRIDES,
#     aws_conn_id="aws-conn",
#     dag=dag,
# )

# step_adder_new_cluster = EmrAddStepsOperator(
#     task_id="step_adder_new_cluster",
#     job_flow_id= "{{ task_instance.xcom_pull(task_id = 'create_job_flow', key='return_value') }}",
#     aws_conn_id="aws-conn", trigger_rule=TriggerRule.ONE_SUCCESS,
#     steps=SPARK_STEPS
# )

step_adder_existing_cluster = EmrAddStepsOperator(
    task_id="step_adder_existing_cluster",
    job_flow_id= cluster_id,
    aws_conn_id="aws-conn",
    steps=SPARK_STEPS
)

# job_complete_new_cluster = EmrStepSensor(
#     task_id='job_complete_new_cluster',
#     job_flow_id= "{{ task_instance.xcom_pull(task_id = 'create_job_flow', key='return_value') }}",
#     step_id= "{{ task_instance.xcom_pull(task_id = 'step_adder_new_cluster', key='return_value')}}",
#     aws_conn_id='aws-conn'
# )

job_complete_existing_cluster = EmrStepSensor(
    task_id='job_complete_existing_cluster',
    job_flow_id= cluster_id,
    step_id= "{{ task_instance.xcom_pull(task_ids = 'step_adder_existing_cluster', key='return_value')["
    + '0' + "] }}",
    aws_conn_id='aws-conn'
)


check >> step_adder_existing_cluster >> job_complete_existing_cluster


