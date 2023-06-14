from datetime import datetime, timedelta

from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession, Window

import pyspark.sql.functions as F

TABLE_NAMES = ['csgomarket', 'lisskins', 'pairs', 'skinbaron', 'tradeit']
MIGRATION_TABLE_NAMES = ['csgomarket', 'lisskins', 'skinbaron', 'tradeit']


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

    def jdbc_write(self, data: DataFrame):
        data.write \
            .format('jdbc') \
            .option('url', 'jdbc:postgresql://192.168.0.29:5432/market') \
            .option("driver", "org.postgresql.Driver")\
            .option('dbtable', 'market_agg.market_history') \
            .option("user", "postgres") \
            .option("password", "changeme") \
            .mode('append') \
            .save()

    def s3_write(self, df, path):
        df.write.mode('overwrite').parquet('s3a://market/' + path)

    def s3_read(self, path):
        return self._spark.read.parquet('s3a://market/' + path)

    def create_path(self, table):
        return (datetime.today() - timedelta(days=1)).strftime('%Y/%m/%d/') + table

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

    def legacy_agg_migration(self):
        etln = self.s3_read('item_etln')

        for table in MIGRATION_TABLE_NAMES:
            for date in range(20230607, 20230613):
                date = datetime.strptime(str(date), '%Y%m%d')
                self.do_agg_migration(etln, date, table)

    def agg_migration(self):
        etln = self.s3_read('item_etln')

        for table in MIGRATION_TABLE_NAMES:
            date = datetime.today() - timedelta(days=1)
            self.do_agg_migration(etln, date, table)

    def do_agg_migration(self, etln, date, table):
        path = date.strftime('%Y/%m/%d/') + table
        df = self.s3_read(path)
        volatility = self.item_volatility(df)
        purchased_by_day = self.purchased_by_day(df)
        avg_price = self.avg_price(df)
        result = etln.join(volatility, volatility.item_key == etln.item_key, 'inner') \
            .join(purchased_by_day, purchased_by_day.item_key == etln.item_key, 'inner') \
            .join(avg_price, avg_price.item_key == etln.item_key, 'inner') \
            .select('name', 'quality', F.coalesce('stattrack', F.lit(False)).alias('stattrack'),
                    F.round('volatility', 3).alias('volatility'), 'purchased_by_day', 'avg_price') \
            .withColumn('period', F.lit(date.date())) \
            .withColumn('market_name', F.lit(table)) \
            .withColumn('weapon_name', F.split(F.regexp_replace(F.col('name'), '★ ', ''), '\s*\|\s*').getItem(0))
        self.jdbc_write(result)

    def avg_price(self, df: DataFrame):
        return df.groupby('item_key').agg(F.round(F.avg('price'), 3).alias('avg_price')).select('item_key', 'avg_price')

    def item_volatility(self, df: DataFrame):
        window = Window.partitionBy("item_key").orderBy(F.col('price_timestamp').asc())

        volatility = df.withColumn('log_price', F.log('price')) \
            .withColumn('diff', F.col('log_price') - F.lag(F.col('log_price'), 1, 0).over(window)) \
            .groupBy('item_key') \
            .agg(F.stddev(F.col('log_price')).alias('volatility'))

        return volatility

    def count_in_pairs(self, df: DataFrame, pairs: DataFrame, source: str):
        source = f'market.{source}'
        agg_pairs = pairs.where((F.col('market_1') == source) | (F.col('market_2') == source)) \
            .groupby('item_key').agg(F.count('item_key').alias('counter')) \
            .select('item_key', 'counter')

        df_pairs = df.join(agg_pairs, agg_pairs.item_key == df.item_key, 'inner') \
            .select(df.item_key, 'counter') \
            .dropDuplicates(['item_key'])

        return df_pairs

    def purchased_by_day(self, df: DataFrame):
        listColumns = df.columns
        window = Window.partitionBy('item_key').orderBy(F.col('price_timestamp').desc())

        if 'market_cup' in listColumns:
            purchased_by_day = df.withColumn(
                'delta_purchase',
                F.col('market_cup') - F.lag(F.col('market_cup'), 1, 0).over(window)
            ).withColumn('delta_purchase', F.when(F.col('delta_purchase') < 0, 0).otherwise(F.col('delta_purchase'))) \
                .groupby('item_key') \
                .agg(F.sum('delta_purchase').alias('purchased_by_day')) \
                .select('item_key', 'purchased_by_day')
        else:
            purchased_by_day = df.groupby('item_key', 'price_timestamp') \
              .agg(F.count('item_key').alias('market_cup')) \
              .withColumn('delta_purchase', F.col('market_cup') - F.lag(F.col('market_cup'), 1, 0).over(window)) \
              .withColumn('delta_purchase', F.when(F.col('delta_purchase') < 0, 0).otherwise(F.col('delta_purchase'))) \
              .groupby('item_key') \
              .agg(F.sum('delta_purchase').alias('purchased_by_day')) \
              .select('item_key', 'purchased_by_day')

        return purchased_by_day


if __name__ == "__main__":
    spark = SparkCustomeSession()
    # spark._spark.sparkContext.setLogLevel('DEBUG')
    spark.legacy_agg_migration()
