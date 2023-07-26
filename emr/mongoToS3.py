from pyspark.sql import SparkSession
import argparse
from datetime import datetime


def getArgs():
    parser = argparse.ArgumentParser(description='spark command line arguments')
    parser.add_argument('--mongouri', type=str, required=True, help= "mongodb ")
    parser.add_argument('--database', type=str, required=True, help= "mongo source table")
    parser.add_argument('--collection', type=str, required=True, help= "database user")
    parser.add_argument('--s3_output_path', type=str, required=True, help= "s3 destination")
    parser.add_argument('--partition_by', type=str, required=False, default=None, help= "column used to partition parquet file during write")
    print("parsing command line arguments")
    return parser.parse_args()


def readFromMongo(spark, mongouri, database, collection):
    print("reading from mongo")
    dataframe = (spark.read.format("com.mongodb.spark.sql.DefaultSource")
                 .option("uri", mongouri).option("database", database).option("collection", collection)
                 # .option("readPreference.name", "primaryPreferred")
                 .option("inferSchema", "true")
                 .load())


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


mongouri = args.mongouri
database = args.database
collection = args.collection

print("Spark Connection starting")
spark = (SparkSession.builder
         .appName(f"MONGO_S3_{collection}_{datetime.now().strftime('%Y%m%d%H%M%S')}")
         .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = readFromMongo(spark, mongouri, database, collection)

writeTos3(df, s3_output_path, partition_by)

spark.stop()
