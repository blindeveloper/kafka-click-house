## Real-time event streaming from Kafka to ClickHouse using Python.

### Stack Overview
- Kafka (Broker + Zookeeper) → event streaming
- ClickHouse → OLAP database for fast analytics
- Python → generate events & consume from Kafka → insert into ClickHouse
- Docker & Docker Compose → run Kafka + ClickHouse locally
- zookeeper → Kafka dependency for managing brokers

### 1. Start services
`docker-compose up -d`

localhost:9092 - Kafka Broker
localhost:8123 - ClickHouse HTTP
localhost:9000 - ClickHouse client program

### 2. access to clickhouse client:
`docker-compose exec clickhouse clickhouse-client`

### 3. create ClickHouse Table Schema:
```sql
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE analytics.events (
    event_time DateTime,
    user_id String,
    event_type String,
    value Float32
) ENGINE = MergeTree()
ORDER BY event_time;
```

### 4. Create Python Virtual Environment & Install dependencies
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 5. Run Event Consumer to insert into ClickHouse
```bash
python consumer.py
```

### 6. Run Event Producer
```bash
python producer.py
```     

### 7. Check last 20 inserted data in ClickHouse
```sql
docker exec -it kafka-click-house-clickhouse-1 clickhouse-client
USE analytics;
SELECT * FROM events LIMIT 20;
```

### 8. Aggregate data in ClickHouse
```sql
SELECT event_type, COUNT(*) AS cnt
FROM events
GROUP BY event_type
ORDER BY cnt DESC;
```
