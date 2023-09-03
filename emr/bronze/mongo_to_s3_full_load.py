from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
import argparse
from datetime import datetime


def get_args():
    parser = argparse.ArgumentParser(description='spark command line arguments')
    parser.add_argument('--mongouri', type=str, required=True, help="mongodb")
    parser.add_argument('--database', type=str, required=True, help="mongo source table")
    parser.add_argument('--collection', type=str, required=True, help="database user")
    parser.add_argument('--s3_output_path', type=str, required=True, help="s3 destination")
    parser.add_argument('--dropColumns', type=list, required=False, default=None, help="columns to drop")
    parser.add_argument('--partition_by', type=str, required=False, default=None, help= "the timestamp columns for partitioning")
    # parser.add_argument('--overwrite_schema', type=bool, required=False, default= False, choices=[True, False])
    # parser.add_argument('--schema', type=str, required=False, default=None, help="to specify columns and data types")
    print("parsing command line arguments")
    return parser.parse_args()


def read_from_mongo(spark, mongouri, database, collection):
    print("reading from mongo")

    dataframe = (spark.read.format("com.mongodb.spark.sql.DefaultSource")
                 .option("uri", mongouri).option("database", database).option("collection", collection)
                 .option("readPreference.name", "secondaryPreferred")
                 .option("inferSchema", "true")
                 .load())

    print(dataframe.printSchema())
    print(dataframe.show(5))

    return dataframe


def write_to_s3(dataframe, s3_output_path, partition_by, collection, dropColumns):
    bucket_name = f"{s3_output_path}bronze/{collection}"
    print(f"writing to s3 bucket {bucket_name}")

    if partition_by is None and dropColumns is None:
        dataframe.write.format("delta").mode('overwrite').saveAsTable(f"bronze.{collection}")

    elif dropColumns is None and partition_by is not None:
        dataframe_2 = dataframe.withColumn("year", year(col(f"{partition_by}"))).withColumn("month", month(col(f"{partition_by}")))
        dataframe_2.write.format("delta").partitionBy("year", "month").mode('overwrite').saveAsTable(f"bronze.{collection}")

    elif dropColumns is not None and partition_by is not None:
        dataframe_2 = dataframe.withColumn("year", year(col(f"{partition_by}"))).withColumn("month", month(
            col(f"{partition_by}"))).drop(*dropColumns)
        dataframe_2.write.format("delta").partitionBy("year", "month").mode('overwrite').saveAsTable(
            f"bronze.{collection}")

    elif dropColumns is not None and partition_by is None:
        dataframe_2 = dataframe.drop(*dropColumns)
        dataframe_2.write.format("delta").mode('overwrite').saveAsTable(f"bronze.{collection}")

    print("DONE!!!!!!!")

print("collecting command line args....")
args = get_args()

mongouri = args.mongouri
database = args.database
collection = args.collection
s3_output_path = args.s3_output_path
partition_by = args.partition_by

print("Spark Connection starting...")
builder = (SparkSession.builder.appName(f"mongo_s3_bronze_{collection}_{datetime.now().strftime('%Y-%m-%d-%H:%M:%S')}")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
           .enableHiveSupport())

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = read_from_mongo(spark, mongouri, database, collection)

write_to_s3(df, s3_output_path, partition_by, collection)

spark.stop()