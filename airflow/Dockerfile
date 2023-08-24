FROM apache/airflow:latest-python3.10

ENV AIRFLOW_HOME=/opt/airflow

USER airflow

COPY ../requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN pip install apache-airflow[amazon]

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID