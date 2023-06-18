from pyspark.sql import SparkSession
import argparse
from datetime import datetime

print("connecting to database")


def getArgs():
    parser = argparse.ArgumentParser(description='spark command line arguments')
    parser.add_argument('--postgresurl', type=str, required=True, help= "postgres source endpoint made up of database hostname, database port and database name")
    parser.add_argument('--table_name', type=str, required=True, help= "postgres source table")
    parser.add_argument('--database_username', type=str, required=True, help= "database user")
    parser.add_argument('--database_password', type=str, required=True, help= "database password")
    parser.add_argument('--s3_output_path', type=str, required=True, help= "s3 destination")
    parser.add_argument('--partition_by', type=str, required=False, default=None, help= "column used to partition parquet file during write")
    print("parsing command line arguments")
    return parser.parse_args()


def readFromPostgres(spark, postgresurl, table_name, database_username, database_password):
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


def writeTos3(dataframe, s3_output_path, partition_by):
    bucket_name = f"{s3_output_path}/{datetime.now().strftime('%Y%m%d%H%M%S')}"
    print(f"writing to s3 bucket {bucket_name}")
    if partition_by == None:
        dataframe.write.parquet(bucket_name, mode= "overwrite")

    else:
        dataframe.write.partitionBy(partition_by).parquet(bucket_name, mode="overwrite")

    print("DONE!!!!!!!")


args = getArgs()

postgresurl = args.postgresurl
table_name = args.table_name
database_username = args.database_username
database_password = args.database_password
s3_output_path = args.s3_output_path
partition_by = args.partition_by

print("Spark Connection starting")
spark = SparkSession.builder.appName(f"POSTGRESQL_S3_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

s3_output_path = f"{s3_output_path}{table_name}"
df = readFromPostgres(spark, postgresurl, table_name, database_username, database_password)

writeTos3(df, s3_output_path, partition_by)

spark.stop()