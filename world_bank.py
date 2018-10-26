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

