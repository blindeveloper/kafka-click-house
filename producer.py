from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

event_types = ['click', 'purchase', 'view']

while True:
    event = {
        'event_time': datetime.now(timezone.utc).isoformat(),
        'user_id': f'user_{random.randint(1,10)}',
        'event_type': random.choice(event_types),
        'value': round(random.random()*100, 2)
    }
    producer.send('events', value=event)
    print("Sent:", event)
    time.sleep(0.1)