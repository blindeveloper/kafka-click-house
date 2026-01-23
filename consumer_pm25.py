from kafka import KafkaConsumer
import clickhouse_connect
import json
from datetime import datetime

# Connect to ClickHouse
ch_client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password='',
    database='analytics'
)

# Create table if not exists
ch_client.command('''
CREATE TABLE IF NOT EXISTS pm25_events (
    event_time DateTime64(3),
    parameter String,
    sensor_id String,
    location_name String,
    area String,
    gps_lat Float64,
    gps_lon Float64,
    value Float64,
    unit String,
    source String
) ENGINE = MergeTree()
ORDER BY event_time;
''')

# Kafka consumer
consumer = KafkaConsumer(
    'pm25_events',
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
        data['parameter'],
        data['sensor_id'],
        data['location_name'],
        data['area'] or '',
        data['gps_lat'],
        data['gps_lon'],
        data['value'],
        data['unit'],
        data['source']
    ])

    if len(batch) >= 50:
        ch_client.insert(
            'pm25_events',
            batch,
            column_names=[
                'event_time', 
                'parameter',
                'sensor_id',
                'location_name',
                'area',
                'gps_lat',
                'gps_lon',
                'value',
                'unit',
                'source'
            ]
        )
        print(f"Inserted batch of {len(batch)} pm25_events")
        batch = []