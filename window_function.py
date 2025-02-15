# 𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
# You are given a large e-commerce transaction dataset stored in a partitioned format based on country.
# Your task is to count the distinct number of products purchased (product_id) for each customer_id in every country.
# The result should include the country, customer ID, and the distinct product count.

# 𝐬𝐜𝐡𝐞𝐦𝐚 
# # Sample data data = [ ("USA", 101, "P001"), 
# ("USA", 101, "P002"), ("USA", 101, "P001"), 
# ("USA", 102, "P003"), ("USA", 102, "P003"), 
# ("UK", 201, "P004"), ("UK", 201, "P005"), 
# ("UK", 202, "P004"), ("UK", 202, "P005"), ("UK", 202, "P004") ]

 # Define schema and create DataFrame columns = ["country", "customer_id", "product_id"]


import findspark
findspark.init()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

spark = SparkSession.builder.appName("TestApp") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

data = [ ("USA", 101, "P001"), 
("USA", 101, "P002"), ("USA", 101, "P001"), 
("USA", 102, "P003"), ("USA", 102, "P003"), 
("UK", 201, "P004"), ("UK", 201, "P005"), 
("UK", 202, "P004"), ("UK", 202, "P005"), ("UK", 202, "P004") ]

columns = ["country", "customer_id", "product_id"]

df = spark.createDataFrame(data, schema=columns)

result_df = df.groupBy('country','customer_id').agg(countDistinct("product_id").alias("distinct_product_count"))
result_df.show()