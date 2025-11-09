from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, avg, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from influxdb import InfluxDBClient  # ⚠️ Utiliser influxdb v1 client

# 1️⃣ Création de la session Spark
spark = SparkSession.builder \
    .appName("KafkaToInflux") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2️⃣ Schéma des données JSON
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# 3️⃣ Lecture depuis Kafka
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# 4️⃣ Extraction et parsing du JSON
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# 5️⃣ Conversion du timestamp
parsed = parsed.withColumn("ts", to_timestamp(col("timestamp")))

# 6️⃣ Agrégation par fenêtre d'une minute
agg = parsed.withWatermark("ts", "2 minutes") \
    .groupBy(window(col("ts"), "1 minute"), col("client_id")) \
    .agg(avg("amount").alias("avg_amount"))

# 7️⃣ Fonction d'écriture dans InfluxDB v1
INFLUX_HOST = "influxdb"
INFLUX_PORT = 8086
INFLUX_USER = "admin"
INFLUX_PASSWORD = "admin123"
INFLUX_DB = "fraud_data"

def write_to_influx(batch_df, batch_id):
    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, username=INFLUX_USER, password=INFLUX_PASSWORD, database=INFLUX_DB)
    
    points = []
    for row in batch_df.collect():
        points.append({
            "measurement": "transactions_avg",
            "tags": {
                "client_id": row["client_id"]
            },
            "time": row["window"]["start"].isoformat(),
            "fields": {
                "avg_amount": float(row["avg_amount"])
            }
        })
    
    if points:
        client.write_points(points)
    
    client.close()
    print(f"✅ Batch {batch_id} sauvegardé dans InfluxDB.")

# 8️⃣ Lancement du stream
query = agg.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_influx) \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

# 9️⃣ Affichage console pour debug
console = agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
console.awaitTermination()
