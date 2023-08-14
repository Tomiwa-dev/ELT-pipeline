# elt-pipeline
This is a repository for a data lakehouse built on s3 using delta lake. 

The main.tf file sets up 10 different AWS services. 
The EMR cluster uses glue catalog as its spark metastore, the cluster also has auto-termination set to 30 mins.
The applications bundled into the EMR clster are ['Spark', 'Trino', 'Zeppelin'] you can edit the main.tf for your preferred applications.

Note: You will need to create an ec2 key-pair. Navigate to the ec2 console on aws, select 
Key Pair in the list of resources, you will then see the option to create one. After downloading
the newly created key pair you might need to run chmod 400 <location-of-key-pair>.

### Usage
First you will need to set up your cloud environments. Clone the repository and change directory to the 
terraform folder.

- run `terraform-init`
- create a new file `secrets.tfvars` in this file you will need to set the values following variables
"aws_secret_key", "aws_access_key", "db_username", "db_password", "aws_key_name"
- run `terraform plan --var-file=secrets.tfvars` to see all the resources terraform would create.
- run `terraform apply ---var-file=secrets.tfvars`
- Follow the guide in the emr cluster console on how to ssh into your cluster.
- To access the spark-shell use the command below `spark-shell --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"`. 

If you want to access a mongo database 
`spark-shell --conf "spark.jars.packages=org.mongodb.spark:mongo-spark-connector_2.12:3.0.1" --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"`