from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import col, column, udf, unix_timestamp
from pyspark.sql.functions import desc
spark = SparkSession.builder.master('local').appName('world_bank').getOrCreate()
df = spark.read.format('csv')\
    .option('inferSchema', 'true')\
    .option('header', 'false')\
    .load('World_Bank_Data.csv')

'''  Remove ',' within the numbers in column to cast it to LongType '''

df2 = df.withColumn('Date', f.from_unixtime(unix_timestamp(col('_c1'),'MM/dd/yyyy')).cast(DateType()))\
    .withColumn('Total_Population', f.regexp_replace(df['_c10'], ',', '').cast(LongType()))

''' Highest urban population - Country having the highest urban population'''
max_urban = df2.groupBy(df2['_c0'].alias('Country'))\
    .agg(f.max('_c11').alias('max_urban'))\
    .sort(desc('max_urban'))\
    .show(1)
    # .limit(1).collect()
# print(max_urban)

''' Most populous Countries - List of countries in the descending order of their population'''
top_pop = df2.groupBy(col('_c0').alias('Country'))\
    .agg(f.sum(col('Total_Population')).alias('Highest_Population'))\
    .sort(desc('Highest_Population'))\
    .show()
