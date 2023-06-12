import os

from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime, timedelta

TABLE_NAMES = ['csgomarket', 'lisskins', 'pairs', 'skinbaron', 'tradeit']


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

    def jdbc_read(self, query: str):
        df = self._spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://192.168.0.29:5432/market") \
            .option("dbtable", query) \
            .option("user", "postgres") \
            .option("password", "changeme") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        df.printSchema()
        return df

    def s3_write(self, df, path):
        df.write.mode('overwrite').parquet('s3a://market/' + path)

    def s3_read(self, path):
        return self._spark.read.parquet('s3a://market/' + path)

    def create_path(self, table):
        return datetime.today().strftime('%Y/%m/%d/') + table

    def create_migrartion_query(self, date, table):
        return f'''
        (with data_per_day as ( 
             select 
              * 
             from market.{table} 
             where price_timestamp between to_date('{date}', 'YYYYMMDD')::timestamp and to_date('{date}', 'YYYYMMDD') + interval '1 day - 1 millisecond'  
            ) 
            select * from data_per_day) as df
        '''

    def legacy_migration(self):
        for table in TABLE_NAMES:
            for date in range(20230607, 20230612):
                query = self.create_migrartion_query(date, table)
                df = self.jdbc_read(query)
                path = datetime.strptime(str(date), '%Y%m%d').strftime('%Y/%m/%d/') + table
                print(path)
                self.s3_write(df, path)

    def do_migration(self):
        for table in TABLE_NAMES:
            now = datetime.now() - timedelta(days=1)
            query = self.create_migrartion_query(now.strftime('%Y%m%d'), table)
            df = self.jdbc_read(query)
            path = self.create_path(table)
            print(path)
            self.s3_write(df, path)

    def migrate_item_etln(self):
        df = self.jdbc_read('(select * from market.item_etln) as df')
        self.s3_write(df, 'item_etln')


if __name__ == "__main__":
    spark = SparkCustomeSession()
    # spark._spark.sparkContext.setLogLevel('DEBUG')
    spark.do_migration()
