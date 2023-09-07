from datetime import timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor, EmrJobFlowSensor
from airflow.utils.dates import days_ago
# from dags.check_for_cluster import check_cluster

mongouri = Variable.get('mongouri')
database = '<>'
collection = '<>'
s3_bucket = Variable.get('s3_bucket')
cluster_id = '<>'

print(mongouri)

default_args = {
    "owner": "Tomiwa",
    "start_date": days_ago(1),
    "depends_on_past": False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=4)
}

job_name = 'mongo_to_s3'
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
            'Args': ['spark-submit', '--master', 'yarn', '--packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.2',
                     f'{s3_bucket}/mongo_to_s3_full_load.py', '--mongouri', mongouri,
                     '--database', database, '--collection', collection, '--s3_output_path', s3_bucket
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
    tags=tags, render_template_as_native_obj=True
)

check = EmptyOperator(task_id= "check", dag=dag)

# create_job_flow = EmrCreateJobFlowOperator(
#     task_id="create_job_flow",
#     job_flow_overrides=JOB_FLOW_OVERRIDES,
#     aws_conn_id="aws-conn",
#     dag=dag,
# )


step_adder_existing_cluster = EmrAddStepsOperator(
    task_id="step_adder_existing_cluster",
    job_flow_id= cluster_id,
    aws_conn_id="aws-conn",
    steps=SPARK_STEPS
)


job_complete_existing_cluster = EmrStepSensor(
    task_id= 'job_complete_existing_cluster',
    job_flow_id= cluster_id,
    step_id= "{{ task_instance.xcom_pull(task_ids = 'step_adder_existing_cluster', key='return_value')["
    + '0' + "] }}",
    aws_conn_id='aws-conn'
)



check_job_flow = EmrJobFlowSensor(task_id="check_job_flow", job_flow_id= cluster_id )



check >> step_adder_existing_cluster >> job_complete_existing_cluster
