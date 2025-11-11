from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, avg, count, window, lit,collect_set, size, min, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from influxdb import InfluxDBClient

# 1️⃣ Spark session
spark = SparkSession.builder \
    .appName("KafkaToInflux") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2️⃣ Schéma JSON étendu 
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True), # ⬅️ AJOUTÉ
    StructField("timestamp", StringType(), True),
    StructField("location", StringType(), True), # ⬅️ AJOUTÉ
    StructField("device", StringType(), True),   # ⬅️ AJOUTÉ
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

# 8.5️⃣ NOUVELLE FONCTION: Écrire les transactions brutes dans Influx
def write_to_influx_raw(batch_df, batch_id):
    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, username=INFLUX_USER,
                            password=INFLUX_PASSWORD, database=INFLUX_DB)
    points = []
    for row in batch_df.collect():
        points.append({
            "measurement": "raw_transactions",
            "tags": {
                "client_id": row["client_id"] or "unknown",
                "region": row["region"] or "unknown",
                "merchant_category": row["merchant_category"] or "unknown",
                "device": row["device"] or "unknown"
            },
            "time": row["ts"].isoformat(),
            "fields": {
                "amount": float(row["amount"]),
                "transaction_id": row["transaction_id"],
                "location": row["location"]
            }
        })
    if points:
        client.write_points(points)
    client.close()
    print(f"✅ Batch {batch_id} (raw transactions) écrit dans InfluxDB.")


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


def write_to_influx_travel_alert(batch_df, batch_id):
    """
    Writes "Impossible Travel" alerts to InfluxDB.
    """
    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, username=INFLUX_USER,
                            password=INFLUX_PASSWORD, database=INFLUX_DB)
    points = []
    
    for row in batch_df.collect():
        regions_str = ",".join(row["regions_seen"])
        
        points.append({
            "measurement": "impossible_travel_alerts",
            "tags": {
                "client_id": row["client_id"] or "unknown"
            },
            "time": row["window"]["start"].isoformat(),
            "fields": {
                "regions_seen_str": regions_str, 
                "region_count": len(row["regions_seen"]) 
            }
        })
        
    if points:
        client.write_points(points)
    
    client.close()
    print(f"✅ Batch {batch_id} (Impossible Travel Alerts) écrit dans InfluxDB.")


def write_to_influx_spree_alert(batch_df, batch_id):
    """
    Writes "Suspicious Spending Spree" alerts to InfluxDB.
    """
    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, username=INFLUX_USER,
                            password=INFLUX_PASSWORD, database=INFLUX_DB)
    points = []
    
    for row in batch_df.collect():
        categories_str = ",".join(row["categories_seen"])
        
        points.append({
            "measurement": "spending_spree_alerts",
            "tags": {
                "client_id": row["client_id"] or "unknown"
            },
            "time": row["window"]["start"].isoformat(),
            "fields": {
                "categories_seen_str": categories_str,  
                "category_count": len(row["categories_seen"]) 
            }
        })
        
    if points:
        client.write_points(points)
    
    client.close()
    print(f"✅ Batch {batch_id} (Spending Spree Alerts) écrit dans InfluxDB.")

def write_to_influx_low_high_alert(batch_df, batch_id):
    """
    Writes "Low-High Velocity" alerts to InfluxDB.
    """
    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, username=INFLUX_USER,
                            password=INFLUX_PASSWORD, database=INFLUX_DB)
    points = []
    
    for row in batch_df.collect():
        points.append({
            "measurement": "low_high_velocity_alerts",
            "tags": {
                "client_id": row["client_id"] or "unknown"
            },
            "time": row["window"]["start"].isoformat(),
            "fields": {
                "min_amount": float(row["min_amount"]),
                "max_amount": float(row["max_amount"])
            }
        })
        
    if points:
        client.write_points(points)
    
    client.close()
    print(f"✅ Batch {batch_id} (Low-High Velocity Alerts) écrit dans InfluxDB.")


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

# Stream pour les transactions brutes
query_raw = parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_influx_raw) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/raw") \
    .start()


# Impossible Travel Detection
# 1. Group by client and a 10-minute window
location_anomaly = parsed \
    .withWatermark("ts", "10 minutes") \
    .groupBy(window(col("ts"), "10 minutes"), col("client_id")) \
    .agg(collect_set("region").alias("regions_seen"))

# 2. The Alert Rule: a client was seen in more than 1 region
impossible_travel_alerts = location_anomaly \
    .filter(size(col("regions_seen")) > 1)

query_impossible_travel = impossible_travel_alerts.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_influx_travel_alert) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/impossible_travel") \
    .start()

# NEW: Suspicious Spending Spree (Category Fan-Out)
# 1. Group by client and window, collect categories
category_anomaly = parsed \
    .withWatermark("ts", "20 minutes") \
    .groupBy(window(col("ts"), "15 minutes"), col("client_id")) \
    .agg(collect_set("merchant_category").alias("categories_seen"))
# 2. The Alert Rule: a client was seen in > 3 categories
spending_spree_alerts = category_anomaly \
    .filter(size(col("categories_seen")) > 3)
# 3. Write this to InfluxDB
query_spending_spree = spending_spree_alerts.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_influx_spree_alert) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/spending_spree") \
    .start()


# 💳 NEW: Low-High Velocity (Card Testing)

# 1. Group by client and window, get min and max amounts
low_high_velocity =parsed \
    .withWatermark("ts", "10 minutes") \
    .groupBy(window(col("ts"), "5 minutes"), col("client_id")) \
    .agg(
        min("amount").alias("min_amount"),
        max("amount").alias("max_amount")
    )

# 2. The Alert Rule: min is low AND max is high
low_high_alerts = low_high_velocity \
    .filter((col("min_amount") < 5) & (col("max_amount") > 1000))

# 3. Write this to InfluxDB
query_low_high = low_high_alerts.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_influx_low_high_alert)\
    .option("checkpointLocation", "/tmp/spark-checkpoints/low_high") \
    .start()


# 10️⃣ Console output for debug
console_avg = avg_by_category.writeStream.format("console") \
    .outputMode("update").option("truncate", False).start()
console_count = count_by_region.writeStream.format("console") \
    .outputMode("update").option("truncate", False).start()

query_avg.awaitTermination()
query_count.awaitTermination()
query_raw.awaitTermination()
query_impossible_travel.awaitTermination()
query_spending_spree.awaitTermination()
query_low_high.awaitTermination()
console_avg.awaitTermination()
console_count.awaitTermination()
