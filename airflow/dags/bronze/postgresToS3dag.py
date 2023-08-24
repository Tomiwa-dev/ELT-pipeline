from datetime import timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from check_for_cluster import check_cluster

table_name = 'links_small'
postgresurl = Variable.get('postgresurl')
username = Variable.get('postgres_username')
password = Variable.get('postgres_password')
aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
s3_bucket = Variable.get('s3_bucket')

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

JOB_FLOW_OVERRIDES = Variable.get("JOB_FLOW_OVERRIDES")
SPARK_STEPS = [
 {
        'Name': 'Run PySpark ETL',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--deploy-mode', 'cluster', '--master', 'yarn',
                     '--class', 'org.apache.spark.deploy.SparkSubmit', '--jars',
                     f's3://{s3_bucket}/cl',
                     f's3://{s3_bucket}/postgresToS3.py',
                     '--postgresurl', {postgresurl},
                     '--table_name', {table_name},
                     '--database_username', {username},
                     '--database_password', {password},
                     '--s3_output_path', f's3://{s3_bucket}/'

                     ]
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
    tags=tags #,render_template_as_native_obj=True
)

def _get_cluster_id(ti):
    cluster_id = ti.xcom_pull(task_ids="get_cluster_id")
    if cluster_id == "No cluster":
        return "create_job_flow"
    return "step_adder"


check = EmptyOperator(task_id= "check", dag=dag)

get_cluster_id = PythonOperator(task_id= "get_cluster_id",
                                dag=dag,
                                python_callable=check_cluster,
                                op_kwargs={'aws_access_key_id': aws_access_key_id,
                                           'aws_secret_access_key': aws_secret_access_key})

branch = BranchPythonOperator(
        task_id="branch",
        python_callable=_get_cluster_id
    )


create_job_flow = EmrCreateJobFlowOperator(
    task_id="create_job_flow",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws-conn",
    dag=dag,
)

step_adder_new_cluster = EmrAddStepsOperator(
    task_id="step_adder_new_cluster",
    job_flow_id= "{{ task_instance.xcom_pull(task_id = 'create_job_flow', key='return_value') }}",
    aws_conn_id="aws-conn", trigger_rule=TriggerRule.ONE_SUCCESS,
    steps=SPARK_STEPS
)

step_adder_existing_cluster = EmrAddStepsOperator(
    task_id="step_adder_existing_cluster",
    job_flow_id= "{{ task_instance.xcom_pull(task_id = 'get_cluster_id', key='return_value') }}",
    aws_conn_id="aws-conn", trigger_rule=TriggerRule.ONE_SUCCESS,
    steps=SPARK_STEPS
)

job_complete_new_cluster = EmrStepSensor(
    task_id='job_complete_new_cluster',
    job_flow_id= "{{ task_instance.xcom_pull(task_id = 'create_job_flow', key='return_value') }}",
    step_id= "{{ task_instance.xcom_pull(task_id = 'step_adder_new_cluster', key='return_value')}}",
    aws_conn_id='aws-conn'
)

job_complete_existing_cluster = EmrStepSensor(
    task_id='job_complete_existing_cluster',
    job_flow_id= "{{ task_instance.xcom_pull(task_id = 'get_cluster_id', key='return_value') }}",
    step_id= "{{ task_instance.xcom_pull(task_id = step_adder_existing_cluster, key='return_value')}}",
    aws_conn_id='aws-conn'
)

check >> get_cluster_id >> branch >> [create_job_flow, step_adder_existing_cluster]
#
create_job_flow >> step_adder_new_cluster >> job_complete_new_cluster
#
step_adder_existing_cluster >> job_complete_existing_cluster
