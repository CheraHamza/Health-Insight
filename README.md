# Health-Insight 🏥

A real-time healthcare analytics platform that monitors patient vital signs, detects anomalies, and provides comprehensive health insights through a modern dashboard interface.



##  Overview

Health-Insight is a comprehensive healthcare monitoring system designed to:

- **Ingest** high-velocity patient vital signs in real-time
- **Process** streaming data to detect critical health anomalies
- **Store** time-series health metrics and patient profiles
- **Analyze** daily health statistics across the hospital
- **Visualize** patient data through an interactive web dashboard

This system demonstrates a modern Lambda Architecture combining real-time stream processing with batch analytics for healthcare data.

---

##  Architecture

The platform uses a multi-tier architecture:

```
┌────────────────┐
│ Data Generator │ 
└──────┬─────────┘
       │
┌──────▼──────┐
│   Kafka     │ 
└──────┬──────┘
       │
┌──────▼──────┐       
│   Spark     │                  
│  Streaming  │
└──────┬──────┘
       │
┌──────▼────────────────┐    ┌──────────────┐
│    Cassandra          │    │   MongoDB    │
│  (Time-Series Data)   │    │  (Profiles)  │
└──┬─────────┬──────▲───┘    └──────┬───────┘
   │         │      │               │
   │      ┌──▼──────┴────────┐      │
   │      │   Spark Batch    │      │
   │      │    Analytics     │      │
   │      └──────────────────┘      │
   │                                │
   │                                │
   └────────────────────────────────┘
              │
        ┌─────▼─────┐
        │   Flask   │
        │ Dashboard │
        └───────────┘
```

### Components:

1. **Data Generator** - Simulates patient vital signs (heart rate, blood pressure, SpO2, temperature, respiratory rate)
2. **Kafka** - Message broker for real-time data ingestion
3. **Spark Streaming** - Real-time anomaly detection and alert generation
4. **Cassandra** - NoSQL database for time-series vitals and alerts
5. **MongoDB** - Document store for patient profiles with replica set for high availability
6. **Spark Batch** - Daily aggregation of hospital-wide health statistics
7. **Flask Dashboard** - Web interface for data visualization and patient management

---

##  Features

-  **Real-Time Monitoring**: Live tracking of 6 vital signs across 20 simulated patients
-  **Automatic Alerts**: Intelligent detection of critical health anomalies
-  **Historical Analysis**: View last 20 readings for any patient metric
-  **Patient Management**: CRUD operations on patient profiles
-  **Risk Scoring**: Dynamic risk assessment based on vital sign patterns
-  **Daily Statistics**: Hospital-wide health metrics aggregation
-  **High Availability**: MongoDB replica set with quorum consistency
-  **Low Latency**: Sub-100ms event ingestion target

---

##  Technology Stack

| Component             | Technology                                |
| --------------------- | ----------------------------------------- |
| **Stream Processing** | Apache Spark 3.5.1 (Structured Streaming) |
| **Message Queue**     | Apache Kafka 4.1.1 (KRaft mode)           |
| **Time-Series DB**    | Apache Cassandra (latest)                 |
| **Document DB**       | MongoDB (3-node replica set)              |
| **Web Framework**     | Flask (Python)                            |
| **Data Generator**    | Kafka Producer (Python)                   |
| **Orchestration**     | Docker Compose                            |
| **Language**          | Python 3.x                                |

---

## Prerequisites

Before you begin, ensure you have the following installed on your system:

- **Docker** and **Docker Compose** 
- **Python** 3.10 or higher
- **Java** 17 (required for Spark)
- **6GB+ RAM** available for Docker containers


##  Installation & Setup

### Step 1: Clone the Repository

```bash
git clone https://github.com/CheraHamza/Health-Insight.git
cd Health-Insight
```

### Step 2: Create Python Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate 
```

### Step 3: Install Python Dependencies

```bash
pip install flask pymongo cassandra-driver kafka-python pyspark pandas
```

### Step 4: Start Infrastructure Services

Start Kafka, Cassandra, and MongoDB using Docker Compose:

```bash
docker-compose up -d
```

This will start:

- Kafka on port **9092**
- Cassandra on port **9042**
- MongoDB replica set on ports **27017**, **27018**, **27019**

**Wait 30-60 seconds** for all services to fully initialize.

### Step 5: Initialize Databases

#### Initialize Cassandra Schema:

```bash
docker exec -i cassandra cqlsh < database/cassandra_schema.cql
```

#### Initialize MongoDB Replica Set & Seed Data:

```bash
# First, initialize the replica set
docker exec -it mongodb-1 mongosh --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"mongodb-1:27017"},{_id:1,host:"mongodb-2:27017"},{_id:2,host:"mongodb-3:27017"}]})'
```

Wait 10 seconds for replica set to stabilize

```bash
# Seed patient profiles
python3 database/mongo_setup.py
```

---

##  Running the Application

The application requires **4 separate terminal windows** (all in the project directory with activated virtual environment):

### Terminal 1: Data Generator (Kafka Producer)

```bash
python3 data_generator/producer.py
```

This simulates patient vital signs and sends them to Kafka every 2-3 seconds.

### Terminal 2: Stream Processing (Spark)

```bash
python3 processing/stream_job.py
```

This processes the stream in real-time, detects anomalies, and writes to Cassandra.

### Terminal 3: Web Dashboard

```bash
python3 -m dashboard.app
```

This starts the Flask web server on port 5000.

### Terminal 4: (Optional) Daily Batch Analytics

```bash
# Process today's data
python3 processing/batch_job.py

# Or specify a date
python3 processing/batch_job.py --stat-date 2025-12-28
```

### Access the Dashboard

Open your browser and navigate to:

```
http://localhost:5000
```

##  Stopping the Application

1. Stop all Python processes (Ctrl+C in each terminal)
2. Stop Docker containers:

```bash
docker-compose down
```

To also remove data volumes:

```bash
docker-compose down -v
```


## ML training (historical)

run `notebooks/ml_risk_model.py` to train ML models on historical data stored in Cassandra.