# 🧠 Real-Time Customer Behavior Dashboard

## 🎯 Project Overview
This project builds a **real-time data pipeline** to monitor and analyze **customer transaction behavior**.  
It processes data from **Kafka → Spark Streaming → InfluxDB → Grafana** to visualize metrics such as:

- 🛍️ **Average transaction amount per category**  
- 👥 **Number of transactions per minute**  
- ⚠️ **Fraud count per merchant category**  
- 📈 **Real-time activity trends**

Unlike ultra-low latency systems (e.g., fraud blocking), this dashboard focuses on **monitoring trends in real time** (latency 1–10 seconds).

---

## 🧩 Architecture

```
Producer → Kafka → Spark Streaming → InfluxDB → Grafana
```

1. **Producer** sends transaction events to a Kafka topic (`transactions`).
2. **Kafka** acts as a real-time message broker.
3. **Spark Structured Streaming** consumes, aggregates, and processes the data stream.
4. **InfluxDB** stores the processed metrics for time-series analysis.
5. **Grafana** visualizes the live metrics via dashboards.

---

## ⚙️ Project Components

| Component | Role | Docker Container |
|------------|------|------------------|
| **Kafka** | Real-time message broker | `kafka` |
| **Spark** | Stream processing engine | `spark-master` |
| **InfluxDB** | Time-series database | `influxdb` |
| **Grafana** | Data visualization tool | `grafana` |

---

## 🚀 How to Run

### 1️⃣ Show data sent by the producer to Kafka
```bash
docker exec -it kafka /usr/bin/kafka-console-consumer     --bootstrap-server localhost:9092     --topic transactions     --from-beginning
```
### delete the topic and create if you need that
PS C:\Users\lanouar> docker exec -it kafka /usr/bin/kafka-topics  --bootstrap-server localhost:9092 --list __consumer_offsets
PS C:\Users\lanouar> docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic transactions --partitions 1 --replication-factor 1
Created topic transactions.

---


### copier le fichier  spark_streaming_app.py  sur le conteneur de spark 
docker cp C:/Users/lanouar/Downloads/fraud_pipeline_project/spark_streaming_app.py spark-master:/opt/spark/work-dir/spark_streaming_app.py 
### 2️⃣ Execute Spark Streaming
Open a terminal inside the Spark container:
```bash
docker exec -it --user root spark-master bash
### 
pip install influxdb
```
Run the Spark Streaming script:
```bash
/opt/spark/bin/spark-submit     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3     spark_streaming_app.py
```

---

### 3️⃣ Display stored data in InfluxDB
```bash
docker exec -it influxdb influx
```

Then in the InfluxDB shell:
```sql
> SHOW DATABASES;
> USE fraud_data;
> SHOW MEASUREMENTS;
> SELECT * FROM transactions_avg LIMIT 10;
```

---

### 4️⃣ Visualize Data in Grafana
1. Go to [http://localhost:3000](http://localhost:3000)
2. Login: `admin / admin`
3. Add a new data source → InfluxDB  
   - URL: `http://influxdb:8086`
   - Database: `fraud_data`
   -username :admin
   -mot de passe :admin123
4. Create dashboards with panels showing:
   - Average amount by merchant category
   - Transactions count over time
---

## 🧱 File Structure

```
📂 project/
├── docker-compose.yml
├── producer.py
├── spark_streaming_app.py
├── requirements.txt
└── README.md
```

---

## 💡 Why This Project Matters
Traditional fraud detection requires **sub-second latency**, which is complex and costly.  
This project instead focuses on **real-time insights** for business and operational visibility.  
It allows companies to:

- Detect **patterns and anomalies** in customer behavior  
- Monitor **live transactions** by category or region  
- Prepare for **predictive analytics and alerting systems**

---

## 🧰 Technologies Used
- Apache **Kafka**
- Apache **Spark Structured Streaming**
- **InfluxDB**
- **Grafana**
- **Docker**

---

## 📊 Next Improvements
- Add geolocation analytics per region
- Integrate machine learning for anomaly detection
- Configure Grafana alerts for specific thresholds
