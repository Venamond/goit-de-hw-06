import random
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.serializer.json import JsonSerializer
from configs import connection_params, SENSORS_TOPIC

producer = KafkaProducer(
    **connection_params(),
    value_serializer=JsonSerializer(),
    key_serializer=JsonSerializer(),
)

# ID датчика однаковий протягом одного запуску скрипту
sensor_id = random.randint(1000, 9999)
temperature = random.uniform(25, 45)
humidity = random.uniform(15, 85)
print(f"Sensor {sensor_id} started -> '{SENSORS_TOPIC}'")

try:
    while True:
        # Значення плавно "блукають", інакше середнє завжди було б посередині діапазону
        # і алерти майже не з'являлись би
        temperature = min(max(temperature + random.uniform(-3, 3), 0), 60)
        humidity = min(max(humidity + random.uniform(-5, 5), 0), 100)

        data = {
            "id": sensor_id,
            "temperature": round(temperature, 1),
            "humidity": round(humidity, 1),
            "timestamp": datetime.now().isoformat(),
        }
        producer.send(SENSORS_TOPIC, key=sensor_id, value=data)
        producer.flush()
        print(f"Sent: {data}")
        time.sleep(2)
except KeyboardInterrupt:
    print("Stopped")
finally:
    producer.close()
