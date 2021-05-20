import json

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


spark = SparkSession.builder \
        .appName('Desafio') \
        .getOrCreate()

df = spark.read.csv('./input/users/', header=True)
# Escolhi parquet por ter bibliotecas default no spark e por ser otimizada com spark SQL
df.repartition(1).write.mode('overwrite').parquet('./output/parquet')
df = spark.read.parquet('./output/parquet/')
df.show()
df = df.withColumn("row_number",F.row_number() \
                       .over(Window.partitionBy(df.id) \
                       .orderBy(df.update_date.desc()))) \
                       .filter(F.col("row_number")==1) \
                       .drop("row_number") \
                       .orderBy(df.id)

f = open('./config/types_mapping.json')
schema = json.load(f)
schema = list(schema.items())

df = df.select(F.col('id'), 
               F.col('name'), 
               F.col('email'), 
               F.col('phone'), 
               F.col('address'), 
               F.col(schema[0][0]).cast(schema[0][1]), 
               F.col(schema[1][0]).cast(schema[1][1]),
               F.col(schema[2][0]).cast(schema[2][1]))

df.repartition(1).write.mode('overwrite').parquet('result')
