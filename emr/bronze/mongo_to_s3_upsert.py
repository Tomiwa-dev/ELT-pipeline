from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import argparse
from datetime import datetime
from delta.tables import DeltaTable
from delta.pip_utils import configure_spark_with_delta_pip



def get_args():
    parser = argparse.ArgumentParser(description='spark command line arguments')
    parser.add_argument('--mongouri', type=str, required=True, help= "mongodb ")
    parser.add_argument('--database', type=str, required=True, help= "mongo source table")
    parser.add_argument('--collection', type=str, required=True, help= "database user")
    parser.add_argument('--s3_table_path', type=str, required=True, help= "s3 table bucket")
    parser.add_argument('--partition_count', type=int, required=False, help = "Number of files to ne generated in s3 bucket")
    parser.add_argument('--partition_by', type=str, required=False, default=None, help= "column used to partition parquet file during write")
    parser.add_argument('--upsert_condition', type=str, required=True, help= "column to merge")

    print("parsing command line arguments")
    return parser.parse_args()


def read_from_mongo(spark, mongouri, database, collection, filter_timestamp):
    print("reading from mongo")
    dataframe = (spark.read.format("com.mongodb.spark.sql.DefaultSource")
                 .option("uri", mongouri).option("database", database).option("collection", collection)
                 # .option("readPreference.name", "primaryPreferred")
                 .option("inferSchema", "true")
                 .load())

    last_update_timestamp = filter_timestamp.collect()
    print(f"The last updated entry was at {last_update_timestamp[0].__getitem__('max(Date)')}")
    dataframe_updates = dataframe.filter(col("Date") >= last_update_timestamp[0].__getitem__('max(Date)'))

    print(dataframe_updates.printSchema())
    print(dataframe_updates.show(5))
    print(dataframe_updates.count())

    return dataframe


def read_delta_table(spark, s3_output_path):
    delta_table = DeltaTable.forPath(spark, s3_output_path)
    delta_df = delta_table.toDF()

    max_date = delta_df.select(max("Date"))

    return delta_table, max_date


def upsert_delta_table(delta_table, new_updates_dataframe, upsert_condition):
    delta_table.alias('dest').merge(new_updates_dataframe.alias('source'), f"source.{upsert_condition} = dest.{upsert_condition}") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()


args = get_args()

uri = args.mongouri
db = args.database
coll = args.collection
s3_path = args.s3_table_path
partition_by = args.partition_by
upsert_con = args.upsert_condition

print("Spark Connection starting")
builder = (SparkSession.builder.appName(f"MONGO_S3_BRONZE_{coll}_{datetime.now().strftime('%Y%m%d%H%M%S')}")
           .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
           .enableHiveSupport())

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

table, last_update = read_delta_table(spark, s3_path)

df = read_from_mongo(spark, uri, db, coll, last_update)

upsert_delta_table(table, df, upsert_con)

spark.stop()


