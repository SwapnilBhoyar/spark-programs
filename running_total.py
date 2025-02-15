# You are given a dataset containing daily stock prices.
# Write a PySpark program to calculate the running total of stock prices for each stock symbol in the dataset.
    
data = [ ("2024-09-01", "AAPL", 150), ("2024-09-02", "AAPL", 160), 
("2024-09-03", "AAPL", 170), ("2024-09-01", "GOOGL", 1200),
 ("2024-09-02", "GOOGL", 1250), ("2024-09-03", "GOOGL", 1300) ] 

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum
spark = SparkSession.builder.appName('testapp') \
.getOrCreate()

df = spark.createDataFrame(data, ["date", "symbol", "price"])

window_spec = Window.partitionBy('symbol').orderBy('date')

df_with_running_total =  df.withColumn('running_total', sum('price').over(window_spec))
df_with_running_total.show()