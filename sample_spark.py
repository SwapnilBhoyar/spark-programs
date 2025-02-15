import findspark
findspark.init()


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestApp") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

# Sample DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

df.show()
# name_list = df.select("name").rdd.flatMap(lambda x: x).collect()
# print(name_list)