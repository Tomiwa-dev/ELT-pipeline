spark-submit --master 'local[*]' --jars '/Users/emmanuel/Documents/Alerzo/dbtPrep/emr/additional_jars/postgresql-42.6.0.jar' '/Users/emmanuel/Documents/personal_projects/dbtProject/emr/postgresToS3.py' --postgresurl jdbc:postgresql://database-1.c051ebytukxg.us-east-1.rds.amazonaws.com:5432/postgres --table_name credits --database_username postgres --database_password postgres --s3_output_path s3://emr-prep-data-lake/movies_metadata/



#spark-submit --master 'local[*]' --jars /Users/emmanuel/jarfiles/mongo-spark-connector-10.0.1.jar --packages org.mongodb.spark:mongo-spark-connector:10.0.1 '/Users/emmanuel/Documents/personal_projects/dbtProject/emr/mongoToS3.py' --mongouri mongodb+srv://tomiwa-dev:Z2zcvaYMLhLbkxwj@cluster0.t5mqmz2.mongodb.net/ --database sample_airbnb --collection listingsAndReviews
