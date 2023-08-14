from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import argparse
from datetime import datetime


def get_args():
    parser = argparse.ArgumentParser(description='spark command line arguments')
    parser.add_argument('--mongouri', type=str, required=True, help="mongodb")
    parser.add_argument('--database', type=str, required=True, help="mongo source table")
    parser.add_argument('--collection', type=str, required=True, help="database user")
    parser.add_argument('--s3_output_path', type=str, required=True, help="s3 destination")
    # parser.add_argument('--file_count', type=int, required=False, default=None, help= "Number of files to be generated in s3 bucket")
    parser.add_argument('--partition_by', type=str, required=False, default=None, help= "column used to partition parquet file during write")
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


def write_to_s3(dataframe, s3_output_path, partition_by, collection):
    bucket_name = f"{s3_output_path}{collection}"
    # {datetime.now().strftime('%Y%m%d%H%M%S')}
    print(f"writing to s3 bucket {bucket_name}")

    if partition_by == None:
        dataframe.write.format("delta").mode('overwrite').save(bucket_name)

    else:
        dataframe.write.format("delta").partitionBy(partition_by).mode('overwrite').save(bucket_name)

    print("DONE!!!!!!!")


args = get_args()

mongouri = args.mongouri
database = args.database
collection = args.collection
s3_output_path = args.s3_output_path
partition_by = args.partition_by

print("Spark Connection starting")
builder = (SparkSession.builder.appName(f"MONGO_S3_BRONZE_{collection}_{datetime.now().strftime('%Y%m%d%H%M%S')}")
           .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
           .enableHiveSupport())

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = read_from_mongo(spark, mongouri, database, collection)

write_to_s3(df, s3_output_path, partition_by, collection)

spark.stop()



spark-submit ./mongo_to_s3.py --mongouri mongodb+srv://tomiwa-dev:DmFeuba92dcX2cPv@cluster0.t5mqmz2.mongodb.net/ --database movies --collection links --s3_output_path s3://emr-prep-data-lake/bronze/
