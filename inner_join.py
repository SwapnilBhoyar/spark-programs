# You are given two DataFrames in PySpark:
# employee_df: Contains employee information.
# department_df: Contains department information.
# You need to perform an inner join on these DataFrames to find out which department each employee belongs to.

import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('testapp') \
    .getOrCreate()

# Sample Employee DataFrame
employee_data = [
    (1, "Alice", 101),
    (2, "Bob", 102),
    (3, "Charlie", 103),
    (4, "David", 101)
]
employee_columns = ["employee_id", "employee_name", "department_id"]
employee_df = spark.createDataFrame(employee_data, employee_columns)

# Sample Department DataFrame
department_data = [
    (101, "HR"),
    (102, "Engineering"),
    (103, "Marketing"),
    (104, "Finance")
]
department_columns = ["department_id", "department_name"]
department_df = spark.createDataFrame(department_data, department_columns)

result_df = employee_df.join(department_df, employee_df.department_id == department_df.department_id, 'right')

result_df = result_df.select(employee_df.employee_id, employee_df.employee_name, employee_df.department_id,result_df.department_name)
result_df.show()
