import os

from pyspark.sql import SparkSession
import pandas as pd

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.hadoop:hadoop-aws:3.3.1," \
                                    "com.amazonaws:aws-java-sdk-bundle:1.12.196" \
                                    " pyspark-shell "


class SparkCustomeSession:
    POSTGRES_PATH = '/home/ubuntu/airflow/MarketCrawler/resources/postgresql-42.6.0.jar'
    HADOOP_PATH = '/home/ubuntu/airflow/MarketCrawler/resources/hadoop-aws-3.3.1.jar'
    S3_PATH = '/home/ubuntu/airflow/MarketCrawler/resources/aws-java-sdk-bundle-1.12.349.jar'

    def __init__(self):
        # TODO Заменить доступ к кредам через переменные окружения
        self._spark = SparkSession.builder \
            .appName("MarketCrawler") \
            .master(f'spark://192.168.0.29:7077') \
            .config("spark.shuffle.service.enabled", "false") \
            .config("spark.hadoop.fs.s3a.endpoint", "https://127.0.0.1:9001") \
            .config("spark.hadoop.fs.s3a.access.key", "4OcB3gSxqzI1Ux1aVXmA") \
            .config("spark.hadoop.fs.s3a.secret.key", "alOms6aHTOODJMvZtJ6LPGrSbAlArgw3ROOYqv7D") \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.dynamicAllocation.enabled", "false") \
            .config("spark.ui.enabled", "false") \
            .config('spark.jars', self.POSTGRES_PATH) \
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

        df.write.format('csv').options(delimiter='|').mode('overwrite').save(
            's3a://market/test.csv')


if __name__ == "__main__":
    spark = SparkCustomeSession()
    spark.s3_write()
