# output:
# ========
# emailid count
# gmail.coom 2
# outlook.com 2
# rediffmail.com 1

import findspark
findspark.init()
from pyspark.sql.functions import col,split,count
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sample") \
.getOrCreate()

data = [("abc12@gmail.coom",),\
 ("bdc12@gmail.coom",),\
 ("psrt23@outlook.com",),\
 ("mno12@rediffmail.com",),\
 ("prt10@outlook.com",)]

columns = ['email']

df = spark.createDataFrame(data, columns)
df_with_split = df.select(split(col('email'),'@').getItem(0).alias('first_part'),
                          split(col('email'),'@').getItem(1).alias('second_part'))

df_with_count = df_with_split.groupBy('second_part').agg(count('second_part').alias('count'))
df_with_count.show()