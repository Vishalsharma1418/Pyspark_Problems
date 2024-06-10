# Databricks notebook source
# MAGIC %md
# MAGIC ### customer who has highest purchase

# COMMAND ----------

from pyspark.sql import SparkSession

spark1=SparkSession.builder\
  .appName("Customer")\
  .config("spark.executor.instances","2")\
  .config("spark.executor.cores","1")\
  .config("spark.executor.memory","1g")\
  .getOrCreate()


# COMMAND ----------

from pyspark.sql.functions import sum,desc
data=[(1,100,"2023-01-15"),
(2,150,"2023-02-20"),
(1,200,"2023-03-10"),
(3,50,"2023-04-05"),
(2,120,"2023-05-15"),
(1,300,"2023-06-25")]

column_name=("customer_id","purchase_amount","purchase_date")
rdd=sc.parallelize(data)
cust_df=spark.createDataFrame(rdd,column_name)
cust_df.show()

grouped_df=cust_df.groupby("customer_id").agg(sum("purchase_amount").alias("total_sum"))
result_df=grouped_df.orderBy(desc("total_sum")).first()
print("The highest tottal is :-",result_df[1])


# COMMAND ----------

print("Number of Executors:", spark1.conf.get("spark.executor.instances"))
print("Executor Cores:", spark1.conf.get("spark.executor.cores"))
print("Executor Memory:", spark1.conf.get("spark.executor.memory"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Passed list in Dataframe**

# COMMAND ----------

list=[('Vishal' ,27),("jony",29)]
spark.createDataFrame(list,['Name','Age']).show()

# COMMAND ----------

# spark.conf.get("spark.sql.files.maxPartitionBytes")
# df.rdd.getNumPartitions()
# spark.sparkContext.defaultParallelism
# spark.conf.get("spark.sql.files.openCostInBytes")

# COMMAND ----------

# MAGIC %md
# MAGIC **Passed dictinoary in DataFrame**

# COMMAND ----------

dict=[{'name' :'Vishal', 'age': 29} ]
spark.createDataFrame(dict).collect()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,avg


# COMMAND ----------

spark=SparkSession.builder\
  .appName("Vishal")\
  .config("spark.executor.instances", "2")\
  .config("spark.executor.cores", "1")\
  .config("spark.executor.memory", "2g")\
  .getOrCreate()

# COMMAND ----------

# To the allocated resources
print("Executors",spark.conf.get("spark.executor.instances"))
print("Core",spark.conf.get("spark.executor.cores"))
print("memory",spark.conf.get("spark.executor.memory"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 1: How do you create a Spark DataFrame from a list of tuples?

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.types import StructType, StructField,IntegerType,StringType

spark1=SparkSession.builder\
  .appName("List_of_tuple")\
  .config("spark.executor.instances", "2")\
  .config("spark.executor.cores","1")\
  .config("spark.executor.memory","1g")\
  .getOrCreate()

# COMMAND ----------

data = [(1, "Alice", 28), (2, "Bob", 25), (3, "Catherine", 30)]
rdd=sc.parallelize(data)

schema=StructType([StructField("id",IntegerType(),True),
                  StructField("name",StringType(),True),
                  StructField("age",IntegerType(),True)
                  ])

df=spark.createDataFrame(rdd,schema)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 2: How do you filter rows in a DataFrame where the age is greater than 25? -->

# COMMAND ----------

filter_df=df.filter(df.age>25)
filter_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 3: How do you group a DataFrame by a column and compute the average age for each group?

# COMMAND ----------

from pyspark.sql.functions import avg
group_df=df.groupBy("name").agg(avg("age").alias("averag_age"))
group_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 4: How do you add a new column to a DataFrame which is the result of a function applied to an existing column?

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def increment_age(age):
  return age+1

incrment_udf=udf(increment_age,IntegerType())

udf_df=df.withColumn("new_age",incrment_udf(df.age))
udf_df.show()





# COMMAND ----------

# MAGIC %md
# MAGIC ####Question 5: How do you join two DataFrames on a common column?

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

data1 = [(1, "Tom", 27), (2, "Jerry", 25), (3, "Alley", 30)]
rdd=sc.parallelize(data1)
age_schema=StructType([ StructField("id", IntegerType(), True),
                       StructField("name",StringType(),True),
                       StructField("age",IntegerType(), True) ])
df1=spark.createDataFrame(data1,age_schema)
df1.show()


joined_df=df.join(df1,"id" ,'inner')
joined_df.show()

# COMMAND ----------


