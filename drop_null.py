#  How would you handle null values in a DataFrame? For example, drop rows with null values in the age column. 

data = [("Alice", 30), 
 ("Bob", None), 
 ("Catherine", 25), 
 (None, 35), 
 ("Eve", None)]

columns = ["Name", "Age"]

import findspark
findspark.init()
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("testapp") \
.config("spark.driver.memory", "1g") \
.config("spark.executor.memory", "1g") \
.getOrCreate()

df = spark.createDataFrame(data, schema = columns)

result_df = df.dropna(subset=["Age"])
result_df.show()
spark.stop()