# Create a new column Category that categorizes people
# based on their age:
# If age is less than 30, the category is Young.
# If age is between 30 and 40, the category is Mid-age.
# If age is greater than 40, the category is Senior

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit, concat

spark = SparkSession.builder.appName('testapp') \
.getOrCreate()

data = [
    ("Alice", 25, 50000),
    ("Bob", 30, 60000),
    ("Charlie", 28, 55000),
    ("David", 35, 70000)]

columns = ['name', 'age', 'salary']

df = spark.createDataFrame(data, schema=columns)

df_with_category = df.withColumn('category', when(col('age')<30, 'young') \
                                 .when((col('age')>=30) & (col('age')<40),'mid-age') \
                                    .otherwise('senior'))

df_with_category.show()

#-------------------------------------------------------------------------

# Creating a New Column by
# Combining Two Columns

df_with_full_info = df.withColumn('full info', concat(col('name'),lit(' is '),col('age'),lit('years old')))
df_with_full_info.show()

spark.stop()