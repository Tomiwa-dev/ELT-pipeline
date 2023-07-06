# elt-pipeline
This is a repository for an elt pipeline. Data is moved from a postgres database to s3 and from s3 to redshift. 

Folder Structure

airflow_local/ - directory for airflow dags and additional scripts and files.
  dags/ - folder for airflow dags.
  
  check_for_cluster.py - this scripts has a function that checks for running emr cluster and returns the cluster id if there is an avaliable cluster or returns a "No cluster" status.
