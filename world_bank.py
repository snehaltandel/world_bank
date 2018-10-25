from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import col, column
from pyspark.sql.functions import desc
# from pyspark.sql.functions import

spark = SparkSession.builder.master('local').appName('world_bank').getOrCreate()
schema = StructType([StructField('Country Name', StringType(), True),
                     StructField('Date', StringType(), False),
                     StructField('Transit: Railways, (million passenger-km)', StringType(), False),
                     StructField('Transit: Passenger cars (per 1,000 people)', StringType(), False),
                     StructField('Business: Mobile phone subscribers', StringType(), False),
                     StructField('Business: Internet users (per 100 people)', StringType(), False),
                     StructField('Health: Mortality', StringType(), False),
                     StructField('under-5 (per 1,000 live births)', StringType(), False),
                     StructField('Health: Health expenditure per capita (current US$)', StringType(), False),
                     StructField('Health: Health expenditure, total (% GDP)', StringType(), False),
                     StructField('Population: Total (count)', StringType(), False),
                     StructField('Population: Urban (count)', StringType(), False),
                     StructField('Population:: Birth rate, crude (per 1,000)', StringType(), False),
                     StructField('Health: Life expectancy at birth, female (years)', StringType(), False),
                     StructField('Health: Life expectancy at birth, male (years)', StringType(), False),
                     StructField('Health: Life expectancy at birth, total (years)', StringType(), False),
                     StructField('Population: Ages 0-14 (% of total)', StringType(), False),
                     StructField('Population: Ages 15-64 (% of total)', StringType(), False),
                     StructField('Population: Ages 65+ (% of total)', StringType(), False),
                     StructField('Finance: GDP (current US$)', StringType(), False),
                     StructField('Finance: GDP per capita (current US$)', StringType(), False)])
df = spark.read.option('delimiter', ',').schema(schema).csv('data.csv')
# df.cache()

df = df.withColumn('Date', df['Date'].cast(DateType()))\
    .withColumn('Population: Urban (count)', df['Population: Urban (count)'].cast(LongType()))\
    .withColumn('Population: Total (count)', df['Population: Total (count)'].cast(LongType()))\
    .withColumn('Population:: Birth rate, crude (per 1,000)', df['Population:: Birth rate, crude (per 1,000)'].cast(DoubleType()))\
    .withColumn('Finance: GDP (current US$)', df['Finance: GDP (current US$)'].cast(DoubleType()))\
    .withColumn('Finance: GDP per capita (current US$)', df['Finance: GDP per capita (current US$)'].cast(DoubleType()))\
    .withColumn('Business: Internet users (per 100 people)', df['Business: Internet users (per 100 people)'].cast(DoubleType()))
df.printSchema()
df.show()
max_urban = df.agg(f.max('Population: Urban (count)').alias('max_urban')).show()
