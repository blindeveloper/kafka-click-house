from kafka import KafkaConsumer
import clickhouse_connect
import json
from datetime import datetime

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password='',
    database='analytics'
)

# Create table if not exists
client.command('''
CREATE TABLE IF NOT EXISTS events (
    event_time DateTime64(3),
    event_type String,
    user_id String,
    value UInt32
) ENGINE = MergeTree()
ORDER BY user_id
''')

# Kafka consumer
consumer = KafkaConsumer(
    'events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

batch = []
for msg in consumer:
    data = msg.value

    event_time = datetime.fromisoformat(data['event_time'])

    batch.append([
        event_time,
        data['event_type'],
        data['user_id'],
        data['value']
    ])

    if len(batch) >= 50:
        client.insert(
            'events',
            batch,
            column_names=['event_time', 'event_type', 'user_id', 'value']
        )
        print(f"Inserted batch of {len(batch)} events")
        batch = []