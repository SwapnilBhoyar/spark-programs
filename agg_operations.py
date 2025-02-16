# You are given a DataFrame containing customer transactions.
# The columns are customer_id, transaction_date, and amount.

# Write a PySpark code to calculate the following:
# The total transaction amount for each customer.
# The average transaction amount for each customer.
# The number of transactions made by each customer.
# Filter out customers who have made more than 3 transactions.


data = [
    (101, "2024-01-01", 500),
    (102, "2024-01-02", 700),
    (101, "2024-01-05", 300),
    (103, "2024-01-07", 1000),
    (104, "2024-01-10", 200),
    (101, "2024-01-15", 450),
    (102, "2024-01-18", 800),
    (103, "2024-01-20", 600),
    (101, "2024-01-22", 350),
    (104, "2024-01-25", 300),
    (101, "2024-01-28", 400),
    (103, "2024-01-30", 700),
    (102, "2024-02-02", 500),
    (102, "2024-02-05", 600),
    (102, "2024-02-08", 750),
    (103, "2024-02-10", 650),
    (103, "2024-02-12", 400)
]

# Create DataFrame
columns = ["customer_id", "transaction_date", "amount"]

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, filter

spark = SparkSession.builder.appName('testapp') \
.getOrCreate()

df = spark.createDataFrame(data, schema=columns)

result_df = df.groupBy('customer_id').agg(sum('amount').alias('total_transaction') \
                                          , avg('amount').alias('avg_transaction') \
                                            , count('customer_id').alias('no_of_transaction'))
# result_df.show()

filtered_df = result_df.filter(result_df.no_of_transaction>3)

filtered_df.show()

spark.stop()