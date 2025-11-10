from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, avg, count, window, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from influxdb import InfluxDBClient

# 1️⃣ Spark session
spark = SparkSession.builder \
    .appName("KafkaToInflux") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2️⃣ Schéma JSON étendu avec category et region
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("region", StringType(), True)
])

# 3️⃣ Lecture depuis Kafka
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# 4️⃣ Extraction JSON
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# 5️⃣ Conversion timestamp
parsed = parsed.withColumn("ts", to_timestamp(col("timestamp")))

# 6️⃣ Ajout temporaire de category et region si absent
parsed = parsed.withColumn("merchant_category", col("merchant_category").cast(StringType()))
parsed = parsed.withColumn("region", col("region").cast(StringType()))

# 7️⃣ Aggregations

# ✅ Average amount by merchant category per minute
avg_by_category = parsed.withWatermark("ts", "2 minutes") \
    .groupBy(window(col("ts"), "1 minute"), col("merchant_category")) \
    .agg(avg("amount").alias("avg_amount"))

# ✅ Transactions count over time per region
count_by_region = parsed.withWatermark("ts", "2 minutes") \
    .groupBy(window(col("ts"), "1 minute"), col("region")) \
    .agg(count("transaction_id").alias("txn_count"))

# 8️⃣ InfluxDB config
INFLUX_HOST = "influxdb"
INFLUX_PORT = 8086
INFLUX_USER = "admin"
INFLUX_PASSWORD = "admin123"
INFLUX_DB = "fraud_data"

def write_to_influx_avg(batch_df, batch_id):
    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, username=INFLUX_USER,
                            password=INFLUX_PASSWORD, database=INFLUX_DB)
    points = []
    for row in batch_df.collect():
        points.append({
            "measurement": "avg_amount_by_category",
            "tags": {
                "merchant_category": row["merchant_category"] or "unknown"
            },
            "time": row["window"]["start"].isoformat(),
            "fields": {
                "avg_amount": float(row["avg_amount"])
            }
        })
    if points:
        client.write_points(points)
    client.close()
    print(f"✅ Batch {batch_id} (avg by category) écrit dans InfluxDB.")

def write_to_influx_count(batch_df, batch_id):
    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, username=INFLUX_USER,
                            password=INFLUX_PASSWORD, database=INFLUX_DB)
    points = []
    for row in batch_df.collect():
        points.append({
            "measurement": "txn_count_by_region",
            "tags": {
                "region": row["region"] or "unknown"
            },
            "time": row["window"]["start"].isoformat(),
            "fields": {
                "txn_count": int(row["txn_count"])
            }
        })
    if points:
        client.write_points(points)
    client.close()
    print(f"✅ Batch {batch_id} (count by region) écrit dans InfluxDB.")

# 9️⃣ Start streaming
query_avg = avg_by_category.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_influx_avg) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/avg") \
    .start()

query_count = count_by_region.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_influx_count) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/count") \
    .start()

# 10️⃣ Console output for debug
console_avg = avg_by_category.writeStream.format("console") \
    .outputMode("update").option("truncate", False).start()
console_count = count_by_region.writeStream.format("console") \
    .outputMode("update").option("truncate", False).start()

query_avg.awaitTermination()
query_count.awaitTermination()
console_avg.awaitTermination()
console_count.awaitTermination()
