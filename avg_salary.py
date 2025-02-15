# Calculate the average salary and count of employees for each department.

# Sample Data
# data = [
#  ("Sales", 5000, "John"),
#  ("Sales", 6000, "Doe"),
#  ("HR", 7000, "Jane"),
#  ("HR", 8000, "Alice"),
#  ("IT", 4500, "Bob"),
#  ("IT", 5500, "Charlie"),
# ]

# df = spark.createDataFrame(data, ["department", "salary", "employee_name"])   

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

# Initialize Spark session
spark = SparkSession.builder.appName("DepartmentSalaryStats").getOrCreate()

# Sample data
data = [
    ("Sales", 5000, "John"),
    ("Sales", 6000, "Doe"),
    ("HR", 7000, "Jane"),
    ("HR", 8000, "Alice"),
    ("IT", 4500, "Bob"),
    ("IT", 5500, "Charlie"),
]

# Define schema
columns = ["Department", "Salary", "Employee"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Compute average salary and count of employees per department
result = df.groupBy("Department").agg(
    avg("Salary").alias("Average_Salary"),
    count("Employee").alias("Employee_Count")
)

# Show the result
result.show()

# Stop Spark session
spark.stop()
