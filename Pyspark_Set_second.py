# Databricks notebook source
# MAGIC %md
# MAGIC ###Dealing with json data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum ,col
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,ArrayType,MapType


# COMMAND ----------

# Create SparkSession 

spark= SparkSession.builder\
       .appName("Json")\
       .config("spark.execcutor.instance","2")\
       .config("spark.executor.core","2")\
       .config("spark.executor.memory","2g")\
       .getOrCreate()

# COMMAND ----------

# Define the data structure as a Python dictionary
data = {
    "name": "PipelineName",
    "properties": {
        "description": "pipeline description",
        "activities": [],
        "parameters": {},
        "concurrency": None,  # Replace with your max pipeline concurrency
        "annotations": []
    }
}

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("properties", StructType([
        StructField("description", StringType(), True),
        StructField("activities", ArrayType(StringType()), True),
        StructField("parameters", MapType(StringType(), StringType()), True),
        StructField("concurrency", IntegerType(), True),
        StructField("annotations", ArrayType(StringType()), True)
    ]), True)
])


# Create DataFrame
df = spark.createDataFrame([data], schema)

# Show DataFrame
df.show(truncate=False)

# COMMAND ----------


