import json

from kafka import KafkaConsumer
from kafka.serializer.json import JsonSerializer
from configs import connection_params, ALERTS_TOPIC

consumer = KafkaConsumer(
    ALERTS_TOPIC,
    **connection_params(),
    value_deserializer=JsonSerializer(),
    auto_offset_reset='earliest',  # показуємо також алерти, що вже накопичились
    group_id='olivin_spark_alert_reader',
)

print(f"Listening to '{ALERTS_TOPIC}'")

try:
    for msg in consumer:
        print(json.dumps(msg.value, indent=2, ensure_ascii=False))
except KeyboardInterrupt:
    print("Stopped")
finally:
    consumer.close()
