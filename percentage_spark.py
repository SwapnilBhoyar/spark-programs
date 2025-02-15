# 𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
# You are given a dataset of sales transactions for multiple stores and products.
# Your task is to calculate the percentage contribution of each product's sales to the total sales of its store.

# 𝐬𝐜𝐡𝐞𝐦𝐚 
# data = [ ("S1", "P1", 100), ("S1", "P2", 200), 
# ("S1", "P3", 300), ("S2", "P1", 400), 
# ("S2", "P2", 100), ("S2", "P3", 500) ]

# Define the schema and create the DataFrame
# columns = ["StoreID", "Product", "Sales"] 

import findspark
findspark.init()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,round

spark = SparkSession.builder.appName("TestApp") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

data =  [ ("S1", "P1", 100), ("S1", "P2", 200), 
("S1", "P3", 300), ("S2", "P1", 400), 
("S2", "P2", 100), ("S2", "P3", 500) ]

columns = ["StoreID", "Product", "Sales"] 

df = spark.createDataFrame(data, schema = columns)

total_sale_df = df.groupBy('StoreID').agg(sum('Sales').alias('TotalSales'))

percentage_df = df.join(total_sale_df,'StoreID')
percentage_df = percentage_df.withColumn('PercentageContribution', round(col('Sales')/col('TotalSales')*100,2))
percentage_df.show()
spark.stop()