from pyspark.sql import SparkSession
import argparse
from datetime import datetime
from delta.pip_utils import configure_spark_with_delta_pip


def get_args():
    parser = argparse.ArgumentParser(description='spark command line arguments')
    parser.add_argument('--postgresurl', type=str, required=True, help= "postgres source endpoint made up of database hostname, database port and database name")
    parser.add_argument('--table_name', type=str, required=True, help= "postgres source table")
    parser.add_argument('--database_username', type=str, required=True, help= "database user")
    parser.add_argument('--database_password', type=str, required=True, help= "database password")
    parser.add_argument('--s3_output_path', type=str, required=True, help= "s3 destination")
    parser.add_argument('--partition_by', type=str, required=False, default=None, help= "column used to partition parquet file during write")
    print("parsing command line arguments")
    return parser.parse_args()


def read_from_postgres(spark, postgresurl, table_name, database_username, database_password):
    print("reading from postgres")
    dataframe = (spark.read.format("jdbc")
                 .option("url", postgresurl)
                 .option("driver", "org.postgresql.Driver")
                 .option("dbtable", table_name)
                 .option("user", database_username)
                 .option("password", database_password).load())

    print(dataframe.printSchema())
    print(dataframe.show(5))

    return dataframe


def write_to_s3(dataframe, s3_output_path, partition_by, table_name):
    bucket_name = f"{s3_output_path}Bronze/{table_name}"
    print(f"writing to s3 bucket {bucket_name}")

    if partition_by is None:
        dataframe.write.format("delta").mode('overwrite').save(bucket_name)

    else:
        dataframe.write.format("delta").partitionBy(partition_by).mode('overwrite').save(bucket_name)

    print("DONE!!!!!!!")


args = get_args()

postgresurl = args.postgresurl
table_name = args.table_name
database_username = args.database_username
database_password = args.database_password
s3_output_path = args.s3_output_path
partition_by = args.partition_by

print("Spark Connection starting")
builder = (SparkSession.builder.appName(f"POSTGRESQL_S3_BRONZE_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}")
           .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
           .enableHiveSupport()
           )

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = read_from_postgres(spark, postgresurl, table_name, database_username, database_password)

write_to_s3(df, s3_output_path, partition_by, table_name)

spark.stop()