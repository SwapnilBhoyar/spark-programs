# How would you use the rank() function in PySpark to rank employees based on their salary within their department?

data = [
    ("Alice", 30, 60000, "Engineering"),
    ("Bob", 25, 50000, "Marketing"),
    ("Charlie", 35, 70000, "Finance"),
    ("David", 28, 55000, "HR"),
    ("Emma", 40, 80000, "Engineering"),
    ("Frank", 32, 62000, "Sales"),
    ("Grace", 29, 58000, "Marketing"),
    ("Hannah", 45, 90000, "Finance"),
    ("Ian", 38, 75000, "HR"),
    ("Jack", 27, 53000, "Sales")
]

columns = ['name', 'age', 'salary', 'department']

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

spark = SparkSession.builder.appName('testapp') \
.getOrCreate()

df = spark.createDataFrame(data, schema=columns)

window_spec = Window.partitionBy('department').orderBy(col('salary').desc())

df_with_rank = df.withColumn('salary_rank', rank().over(window_spec))
df_with_rank.show()

spark.stop()