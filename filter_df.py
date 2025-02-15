# Given a DataFrame df with columns id, name, and salary, write a PySpark
# code to filter rows where salary is greater than 3000 and select only the
# name column.

data = [ (1, "Alice", 2000), (2, "Bob", 3000), 
 (3, "Alice", 2000), (4, "David", 4000), 
 (5, "Alice", 5000), (6, "Bob", 3000) ] 
columns = ["ID", "Name", "Salary"]

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('testapp') \
.getOrCreate()


df = spark.createDataFrame(data, schema=columns)

name_df = df.filter(df['Salary']>3000).select('Name')

name_df.show()