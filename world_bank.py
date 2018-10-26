from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import col, column, udf, unix_timestamp
from pyspark.sql.functions import desc
spark = SparkSession.builder.master('local').appName('world_bank').getOrCreate()
df = spark.read.format('csv')\
    .option('inferSchema', 'true')\
    .option('header', 'false')\
    .load('Data.csv')

''' NOTE:::: Remove ',' within the numbers in column to cast it to LongType to perform aggregations'''
''' Cast column _c1 to DateType and _c10 to LongType'''
df2 = df.withColumn('Date', f.from_unixtime(unix_timestamp(col('_c1'),'MM/dd/yyyy')).cast(DateType()))\
    .withColumn('Total_Population', f.regexp_replace(df['_c10'], ',', '').cast(LongType()))

''' Highest urban population - Country having the highest urban population'''
max_urban = df2.agg(f.max('_c11').alias('max_urban')).show()

''' Most populous Countries - List of countries in the descending order of their population'''
top_pop = df2.groupBy(col('_c0').alias('Country'))\
    .agg(f.sum(col('Total_Population')).alias('Highest_Population'))\
    .sort(desc('Highest_Population'))\
    .show()

'''Highest population growth - Country with highest % population growth in past decade'''


'''Highest GDP growth - List of Countries with highest GDP growth from 2009 to 2010 in descending order'''
'''Internet usage grown - Country where Internet usage has grown the most in the past decade'''
'''Youngest Country - Yearly distribution of youngest Countries'''
