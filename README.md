
#  Real-Time Customer Behavior Dashboard

##  Project Overview
This project builds a real-time data pipeline to monitor and analyze customer transaction behavior. It processes data from Kafka → Spark Streaming → InfluxDB → Grafana to visualize metrics such as:

- 🛍️ Average transaction amount per category
- 👥 Number of transactions per minute
- 📈 Real-time activity trends
- ⚠️ Impossible Travel Alerts
- 🛒 Suspicious Spending Spree Alerts
- 💳 Low-High Velocity (Card Testing) Alerts

Unlike ultra-low latency systems (e.g., fraud blocking), this dashboard focuses on monitoring trends and detecting anomalies in real time (latency 1–10 seconds).

##  Architecture
**Producer → Kafka → Spark Streaming → InfluxDB → Grafana**

- Producer sends transaction events to a Kafka topic (`transactions`).
- Kafka acts as a real-time message broker.
- Spark Structured Streaming consumes, aggregates, and processes the data stream to detect fraud.
- InfluxDB stores the processed metrics and fraud alerts for time-series analysis.
- Grafana visualizes the live metrics and alerts via dashboards.

##  Project Components
| Component | Role | Docker Container |
|------------|------|------------------|
| Kafka | Real-time message broker | kafka |
| Spark | Stream processing & fraud detection | spark-master |
| InfluxDB | Time-series database | influxdb |
| Grafana | Data visualization tool | grafana |

##  Real-Time Fraud Detection
The core of this project is its ability to run real-time fraud detection rules in Spark Streaming. We have implemented three key stateful rules:

1. **Impossible Travel:** Detects when a single client makes transactions in two or more different regions (e.g., 'north' and 'west') within an impossibly short 10-minute window.  
2. **Suspicious Spending Spree:** Flags clients who make purchases across four or more different merchant categories (e.g., "food," "fashion," "travel") within a 15-minute window.  
3. **Low-High Velocity (Card Testing):** Catches a classic fraud pattern where a client makes a very small transaction (< $5) followed immediately by a very large one (> $1000) within the same 5-minute window.

## 🚀 How to Run

### 1️⃣ Show data sent by the producer to Kafka
```bash
docker exec -it kafka /usr/bin/kafka-console-consumer     --bootstrap-server localhost:9092     --topic transactions     --from-beginning
```

To delete and recreate the topic:
```bash
# List topics
docker exec -it kafka /usr/bin/kafka-topics --bootstrap-server localhost:9092 --list

# Create topic
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic transactions --partitions 1 --replication-factor 1
```

### 2️⃣ Execute Spark Streaming
Copy the Spark script to the container:
```bash
docker cp C:/Users/lanouar/Downloads/fraud_pipeline_project/spark_streaming_app.py spark-master:/opt/spark/work-dir/spark_streaming_app.py
```

Open a terminal inside the Spark container and install dependencies:
```bash
docker exec -it --user root spark-master bash
pip install influxdb
```

Run the Spark Streaming script:
```bash
/opt/spark/bin/spark-submit     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3     spark_streaming_app.py
```

### 3️⃣ Display stored data in InfluxDB
```bash
docker exec -it influxdb influx
```
Then in the InfluxDB shell:
```sql
> SHOW DATABASES;
> USE fraud_data;
> SHOW MEASUREMENTS;
> SELECT * FROM "raw_transactions" LIMIT 10;
> SELECT * FROM "impossible_travel_alerts" LIMIT 10;
> SELECT * FROM "spending_spree_alerts" LIMIT 10;
> SELECT * FROM "low_high_velocity_alerts" LIMIT 10;
```

### 4️⃣ Visualize Data in Grafana
Go to [http://localhost:3000](http://localhost:3000)

Login: `admin / admin`

Add a new data source → **InfluxDB**  
```
URL: http://influxdb:8086
Database: fraud_data
User: admin
Password: admin123
```

Create dashboards with panels showing:
- Average amount by merchant category
- Transactions count over time
- Fraud alert tables (Impossible Travel, Spending Spree, etc.)

##  File Structure
```
📂 project/
├── docker-compose.yml
├── producer.py
├── spark_streaming_app.py
├── requirements.txt
├── Grafana_Configuration.json
└── README.md
```

##  Why This Project Matters
Traditional fraud detection requires sub-second latency, which is complex and costly. This project instead focuses on real-time insights for business and operational visibility. It allows companies to:

- Detect patterns and anomalies in customer behavior  
- Monitor live transactions by category or region  
- Provide a powerful, scalable foundation for advanced alerting systems

##  Technologies Used
- Apache Kafka  
- Apache Spark Structured Streaming  
- InfluxDB  
- Grafana  
- Docker  

## 👥 Team Members
- lanouer KHALED :  lanouar.khaled@ensi-uma.tn
- Yasmine RIABI : yasmine.riabi@ensi-uma.tn
- Oussama CHAABANE :  oussama.chaabane@ensi-uma.tn

## 📊 Next Improvements
- Add geolocation analytics per region  
- Integrate machine learning for anomaly detection  
- Configure Grafana alerts for specific thresholds  
