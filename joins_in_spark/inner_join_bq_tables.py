import logging

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

logging.basicConfig(level=logging.INFO)

PROJECT_ID = 'bubbly-delight-397006'
TEMP_BUCKET = "pyspark-bq-connector-temp"
DATASET = 'sample_data_spark'
PROJECTS_TABLE = 'projects'
PERSONS_TABLE = 'persons'
FINAL_TABLE = 'final_table'

spark = SparkSession \
    .builder \
    .appName('spark-bigquery-demo') \
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2') \
    .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
    .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
spark.conf.set('temporaryGcsBucket', TEMP_BUCKET)
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)


def main():
    # Load persons table from BigQuery.
    persons = spark.read.format('bigquery') \
        .option('table', f'{PROJECT_ID}:{DATASET}.{PERSONS_TABLE}') \
        .load()

    # Load projects table from BigQuery.
    projects = spark.read.format('bigquery') \
        .option('table', f'{PROJECT_ID}:{DATASET}.{PROJECTS_TABLE}') \
        .load()

    df = persons.join(other=projects, on='gcp_project_id')
    df.show()

    default_timestamp = datetime.utcnow().isoformat()
    df = df.withColumn("new_column", lit(default_timestamp))
    df.show()

    # Save the data to BigQuery
    df.write.format('bigquery') \
        .option('table', f'{PROJECT_ID}:{DATASET}.{FINAL_TABLE}') \
        .save()


if __name__ == '__main__':
    main()
