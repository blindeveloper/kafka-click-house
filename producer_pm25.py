from kafka import KafkaProducer
import json
import time
import requests

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# API endpoint
API_URL = "https://pm25.lass-net.org/API-1.0.0/project/airbox/latest/"

while True:
    try:
        response = requests.get(API_URL)
        sensors = response.json()["feeds"]  # List of sensor dicts
    except Exception as e:
        print("Error fetching API:", e)
        time.sleep(30)
        continue

    for sensor in sensors:
        base_event = {
            "source": sensor.get("app"),
            "sensor_id": sensor.get("device_id"),
            "location_name": sensor.get("SiteName"),
            "area": sensor.get("area"),
            "gps_lat": sensor.get("gps_lat"),
            "gps_lon": sensor.get("gps_lon"),
            "event_time": sensor.get("timestamp")
        }

        # PM2.5
        pm25_event = base_event.copy()
        pm25_event.update({
            "parameter": "pm25",
            "value": sensor.get("s_d0"),
            "unit": "µg/m3"
        })
        producer.send("pm25_events", pm25_event)

        # Temperature
        temp_event = base_event.copy()
        temp_event.update({
            "parameter": "temperature",
            "value": sensor.get("s_t0"),
            "unit": "°C"
        })
        producer.send("pm25_events", temp_event)

        # Humidity
        hum_event = base_event.copy()
        hum_event.update({
            "parameter": "humidity",
            "value": sensor.get("s_h0"),
            "unit": "%"
        })
        producer.send("pm25_events", hum_event)

    print(f"Produced batch of {len(sensors)} sensors")
    time.sleep(10)  # AirBox updates every ~5 minutes