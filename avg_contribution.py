# To calculate the percentage of total salary that each employee contributes to their respective department.

data = [
    (1, "Alice", "Engineering", 70000),
    (2, "Bob", "Engineering", 80000),
    (3, "Charlie", "HR", 50000),
    (4, "David", "HR", 60000),
    (5, "Emma", "Marketing", 75000),
    (6, "Frank", "Marketing", 72000),
    (7, "Grace", "Finance", 90000),
    (8, "Hannah", "Finance", 85000),
    (9, "Ian", "Engineering", 95000),
    (10, "Jack", "Marketing", 77000)
]

# Create DataFrame
columns = ["employee_id", "name", "department", "salary"]

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, round
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('testapp') \
    .getOrCreate()

df = spark.createDataFrame(data, schema=columns)

window_spec = Window.partitionBy('department')
df_with_sum_salary = df.withColumn('department_sum_sal', sum('salary').over(window_spec))
df_with_contribution = df_with_sum_salary.withColumn('contribution',   round(col('salary')/col('department_sum_sal')*100, 2))
df_with_contribution.show()

spark.stop()