from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    print('Storing random numbers in a hive table')
    # spark.range(100).write.saveAsTable("random_numbers")
    print('complete')

    read_hive(spark_session=spark, table_name='random_numbers')


def read_hive(spark_session, table_name):
    table = spark_session.sql(f'SELECT * FROM {table_name}')

    table.show()


if __name__ == '__main__':
    main()
