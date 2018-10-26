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


