# 𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧 3 
# Different ways to remove duplicates
# Write a PySpark code to remove duplicate rows based on specific columns.

data = [ (1, "Alice", 2000), (2, "Bob", 3000), 
 (3, "Alice", 2000), (4, "David", 4000), 
 (5, "Alice", 5000), (6, "Bob", 3000) ] 
columns = ["ID", "Name", "Salary"]

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, row_number
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("testapp") \
.getOrCreate()


df = spark.createDataFrame(data, schema=columns)

#---------METHOD 1-------------------------
result_df = df.dropDuplicates(['Name', 'Salary'])
# result_df.show()


#----------METHOD 2------------------------

group_df = df.groupBy('Name', 'Salary').agg(first('Id').alias('Id'))
# group_df.show()


#-----------------METHOD 3---------------------


window_spec = Window.partitionBy('Name', 'Salary').orderBy('Id')
df_with_row_num = df.withColumn('row_num', row_number().over(window_spec))
filtered_df = df_with_row_num.filter(df_with_row_num['row_num']==1).drop('row_num')
filtered_df.show()