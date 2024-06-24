# Databricks notebook source
# MAGIC %md
# MAGIC ###Hightest salary by department

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,sum,col,dense_rank
from pyspark.sql.types import StructType, StructField, IntegerType,StringType

# COMMAND ----------

spark= SparkSession.builder.appName("Top highest by department").getOrCreate()

# COMMAND ----------

data=[(1,"Alice",28	,"HR",55000),
      (2,"Bob",	34	,"IT",75000),
      (3,"Charlie",25,"Marketing"	,50000),
      (4,"David",	40 ,"Finance",	90000),
      (5,"Eve",30,	"IT",	70000) 
      ]
schema=("id"	,"name"	,"age",	"department","salary")
rdd=sc.parallelize(data)
df=spark.createDataFrame(rdd,schema)
df.show()

# COMMAND ----------

groupded_df=df.groupBy("department").agg(sum("salary").alias("total_salary"))
final_df=groupded_df.orderBy(col("total_salary").desc())
final_df.show()

# COMMAND ----------

from pyspark.sql.window import Window

window_spec=Window.orderBy(col("total_salary").desc())
rnked_df=final_df.withColumn("rank", dense_rank().over(window_spec)).limit(1)
rnked_df.show()

# COMMAND ----------



