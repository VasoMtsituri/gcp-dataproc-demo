import logging
import re

from google.cloud import storage
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)

PROJECT_ID = 'bubbly-delight-397006'
TEMP_BUCKET = "pyspark-bq-connector-temp"
DATASET = 'sample_data_spark'
CSV1M_GSUTIL_URI = 'gs://anime-reviews-raw/final_animedataset_1M.csv'
CSV100K_GSUTIL_URI = 'gs://anime-reviews-raw/final_animedataset_100K.csv'
# CSV_GSUTIL_URI = 'resources/final_animedataset_1M.csv'
USERNAME_BUCKETS_NAME = 'usernames'
REVIEWS_RAW_BUCKETS_NAME = 'anime-users-csvs'
PARTITIONED_CSVS_GSUTIL_URI = f'gs://{REVIEWS_RAW_BUCKETS_NAME}/{USERNAME_BUCKETS_NAME}'

USERNAME_RGX = r'username=(.*?\/)'

spark = SparkSession \
    .builder \
    .appName('spark-bigquery-demo-local2') \
    .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
    .getOrCreate()


# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
# spark.conf.set('temporaryGcsBucket', TEMP_BUCKET)


def main():
    # df = spark.read \
    #     .option("header", "true") \
    #     .csv(CSV100K_GSUTIL_URI)
    #
    # df.write.option("header", True) \
    #     .partitionBy("username") \
    #     .mode("overwrite") \
    #     .csv(PARTITIONED_CSVS_GSUTIL_URI)

    gcs_client = storage.Client()

    result_iter = gcs_client.list_blobs(bucket_or_name=REVIEWS_RAW_BUCKETS_NAME)

    csvs = [x for x in result_iter if x.name.endswith('.csv')]

    usernames_list = [re.findall(pattern=USERNAME_RGX, string=x.name) for x in csvs]
    usernames = [x[0].rstrip('/') for x in usernames_list if x]

    print(f'csv==usernames: {len(csvs) == len(list(set(usernames)))}')
    print(f'Len(csvs): {len(csvs)}')
    print(f'Len(usernames): {len(usernames)}')
    print(f'Unique Len(usernames): {len(list(set(usernames)))}')

    print(usernames)

    # df = spark.read \
    #     .option("header", "true") \
    #     .csv(PARTITIONED_CSVS_GSUTIL_URI)
    #
    # df.printSchema()
    # print(f'Size: {df.count()}')


if __name__ == '__main__':
    main()
