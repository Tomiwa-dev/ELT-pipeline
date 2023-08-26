import time

import boto3
from datetime import date, datetime
from botocore.config import Config

my_config = Config(
    region_name = 'us-east-1',
    signature_version = 'v4',
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    }
)


year = date.today().year
month = date.today().month
day = date.today().day


def check_cluster(aws_access_key_id, aws_secret_access_key):
    client = boto3.client('emr', config=my_config,aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)

    response = client.list_clusters(
        CreatedAfter=datetime(year, month, day - 1),
        CreatedBefore=datetime(year, month, day + 1),
        ClusterStates=[
            'STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'
        ],
    )

    if len(response['Clusters']) == 0:
        cluster_id = "No cluster"
        return cluster_id

    elif len(response['Clusters']) > 0 and response['Clusters'][0]['Status']['State'] in ['STARTING','BOOTSTRAPPING']:
        time.sleep(300)
        check_cluster(aws_access_key_id, aws_secret_access_key)

    else:
        cluster_id = response['Clusters'][0]['Id']
        return cluster_id


