from pyspark.sql import SparkSession
from pyspark.sql import col, max
from delta import *
from delta.tables import DeltaTable
import argparse

builder = SparkSession.builder.appName("upsertFunction")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


print("connecting to database")

def getArgs():
    parser = argparse.ArgumentParser(description='spark command line arguments')
    parser.add_argument('--mongouri', type=str, required=True, help= "mongodb ")
    parser.add_argument('--database', type=str, required=True, help= "mongo source table")
    parser.add_argument('--collection', type=str, required=True, help= "database user")
    parser.add_argument('--s3_output_path', type=str, required=True, help= "s3 destination")
    parser.add_argument('--partition_by', type=str, required=False, default=None, help= "column used to partition parquet file during write")
    print("parsing command line arguments")
    return parser.parse_args()


#source
def readFromMongo(spark, mongouri, database, collection, filter_timestamp, filter_column):
    print("reading from mongo")
    dataframe = (spark.read.format("mongodb")
                 .option("uri", mongouri).option("database", database).option("collection", collection)
                 # .option("readPreference.name", "primaryPreferred")
                 .option("inferSchema", "true")
                 .load())

    dataframe_updates = dataframe.filter(col(filter_column) > filter_timestamp)

    return dataframe_updates

#dest
delta_table = DeltaTable.forPath(spark, "./upsert_overwrite_mode")
delta_df = delta_table.toDF()

df = (spark.read.format("delta").load(s3_path))
max_update_timestamp = df.select(max("updated_at"))

delta_table.alias('dest').merge(delta_table.alias('source'), "source.id = dest.id")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()


#write to database