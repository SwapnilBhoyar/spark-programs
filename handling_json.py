# Flattening JSON data

# {
#   "id": 1,
#   "name": "Alice",
#   "address": {
#     "city": "New York",
#     "state": "NY"
#   },
#   "contacts": [
#     {"type": "email", "value": "alice@example.com"},
#     {"type": "phone", "value": "1234567890"}
#   ]
# }

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
spark = SparkSession.builder.appName("sample") \
.getOrCreate()


data = [
    (1, "Alice", {"city": "New York", "state": "NY"}, 
     [{"type": "email", "value": "alice@example.com"}, {"type": "phone", "value": "1234567890"}])
]

# Create DataFrame
schema = ["id", "name", "address", "contacts"]
df = spark.createDataFrame(data, schema=schema)
flattened_df = df.select(col('id'),col('name'),col('address.city').alias('city'),col('address.state').alias('state'),explode(col('contacts')).alias('contact'))
flattened_df = flattened_df.select(col('id'),
                                   col('name'),
                                   col('city'),
                                   col('state'),
                                   col('contact.type').alias('contact_type'),
                                   col('contact.value').alias('contact_value'))

flattened_df.show()

spark.stop()