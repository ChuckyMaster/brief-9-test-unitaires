from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, sum

# Créer une session Spark
spark = SparkSession.builder.appName("dep").getOrCreate()

data = [
    (1, 34, 'Cardiology', 10),
    (2, 45, 'Neurology', 12),
    (3, 23, 'Cardiology', 5),
    (4, 64, 'Orthopedics', 8),
    (5, 52, 'Cardiology', 9)
]
columns = ['patient_id', 'age', 'department', 'visit_count']

df_spark = spark.createDataFrame(data, schema=columns)




agg_df_spark = df_spark.groupBy("department").agg(
    sum("visit_count").alias("total_visits"),
    avg("age").alias("average_age"),
    max("age").alias("max_age")
)

agg_df_spark.show()