import os

from pyspark.sql import SparkSession
import pandas as pd


class SparkCustomeSession:
    RESOURCES = '/home/ubuntu/airflow/MarketCrawler/resources'
    JARS_PATH = f'{RESOURCES}/postgresql-42.6.0.jar,' \
                f'{RESOURCES}/aws-java-sdk-bundle-1.12.196.jar,' \
                f'{RESOURCES}/hadoop-aws-3.3.1.jar'

    def __init__(self):
        self._spark = SparkSession.builder \
            .appName("MarketCrawler") \
            .master(f'spark://192.168.0.29:7077') \
            .config("spark.ui.port", "4041") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.0.29:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "4OcB3gSxqzI1Ux1aVXmA") \
            .config("spark.hadoop.fs.s3a.secret.key", "alOms6aHTOODJMvZtJ6LPGrSbAlArgw3ROOYqv7D") \
            .config("spark.hadoop.fs.s3a.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config('spark.jars', self.JARS_PATH) \
            .getOrCreate()

    @property
    def spark(self) -> SparkSession:
        return self._spark

    def jdbc_read(self, table_name: str):
        df = self._spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/market") \
            .option("dbtable", table_name) \
            .option("user", "postgres") \
            .option("password", "changeme") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        df.printSchema()

    def s3_write(self):
        df = self._spark.createDataFrame(pd.DataFrame({'a': [1, 2, 3], 'b': [4, 2, 1]}))
        df.show()
        # df = self._spark.read.csv("s3a://market/wireshark.csv")
        # df.show()
        df.write.format('csv').options(delimiter='|').mode('overwrite').save('s3a://market/test4.csv')


if __name__ == "__main__":
    spark = SparkCustomeSession()
    # spark._spark.sparkContext.setLogLevel('INFO')
    spark.s3_write()
