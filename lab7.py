import time
import shutil
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, avg, count, when

spark = SparkSession.builder.appName("InsuranceStreamAnalysis").getOrCreate()

schema = StructType([
    StructField("age", IntegerType(), True),
    StructField("sex", StringType(), True),
    StructField("bmi", DoubleType(), True),
    StructField("children", IntegerType(), True),
    StructField("smoker", StringType(), True),
    StructField("region", StringType(), True),
    StructField("charges", DoubleType(), True)
])

streaming_path = "C:/Users/Win11/Documents/3RD YEAR/2nd SEM/Big Data Analysis (Azore)/Act-Lab/Lab7/insurance_stream/"
if os.path.exists(streaming_path):
    shutil.rmtree(streaming_path)
os.makedirs(streaming_path)

streaming_path = "insurance_stream/"
if not os.path.exists(streaming_path):
    print(f"Directory '{streaming_path}' not found. Please create it.")

os.makedirs(streaming_path, exist_ok=True)

insurance_df = pd.read_csv("insurance.csv")

def simulate_streaming_data(df, path, batch_size=5, interval=3):
    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i + batch_size]
        batch_file = f"{path}/batch_{int(time.time())}.csv"
        batch_df.to_csv(batch_file, index=False)
        print(f"📡 New batch added: {batch_file}")
        time.sleep(interval)

import threading
thread = threading.Thread(target=simulate_streaming_data, args=(insurance_df, streaming_path, 10, 5))
thread.start()

insurance_stream = (
    spark.readStream
    .schema(schema)
    .csv(streaming_path, header=True)
)

avg_charges_stream = insurance_stream.agg(avg("charges").alias("avg_charges"))

smoker_charges_stream = insurance_stream.groupBy("smoker").agg(avg("charges").alias("avg_smoker_charges"))

region_metrics_stream = insurance_stream.groupBy("region").agg(
    count("*").alias("policy_count"),
    avg("charges").alias("avg_region_charges")
)

age_group_stream = insurance_stream.withColumn(
    "age_group",
    when(col("age") <= 35, "Young")
    .when((col("age") > 35) & (col("age") <= 55), "Middle-aged")
    .otherwise("Senior")
).groupBy("age_group").agg(
    count("*").alias("count"),
    avg("charges").alias("avg_age_group_charges")
)

avg_charges_query = avg_charges_stream.writeStream.outputMode("complete").format("console").start()
smoker_charges_query = smoker_charges_stream.writeStream.outputMode("complete").format("console").start()
region_metrics_query = region_metrics_stream.writeStream.outputMode("complete").format("console").start()
age_group_query = age_group_stream.writeStream.outputMode("complete").format("console").start()

avg_charges_query.awaitTermination()
smoker_charges_query.awaitTermination()
region_metrics_query.awaitTermination()
age_group_query.awaitTermination()
