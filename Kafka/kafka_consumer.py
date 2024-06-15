from kafka import KafkaConsumer
import json,csv,time

consumer = KafkaConsumer('VDT2024',
    auto_offset_reset='earliest',
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
for message in consumer:
    print(f"Received message: {message.value}")
