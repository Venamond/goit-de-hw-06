from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from configs import kafka_config, connection_params, MY_NAME, SENSORS_TOPIC, ALERTS_TOPIC

topics = [SENSORS_TOPIC, ALERTS_TOPIC]

print("=" * 60)
print("  KAFKA: створення топіків".center(60))
print("=" * 60)
print(f"  Сервер : {kafka_config['bootstrap_servers'][0]}")
print(f"  Префікс: {MY_NAME}")
print("-" * 60)

print("[1/3] Підключення до Kafka ...", flush=True)
admin_client = KafkaAdminClient(**connection_params())
print("      OK")

print("[2/3] Створення топіків:")
for i, name in enumerate(topics, start=1):
    print(f"      ({i}/{len(topics)}) {name:<32}", end=" ", flush=True)
    try:
        admin_client.create_topics([NewTopic(name=name, num_partitions=2, replication_factor=1)])
        print("✅ створено")
    except TopicAlreadyExistsError:
        # Повторний запуск не повинен падати
        print("⚠️  вже існує")

print("[3/3] Мої топіки на сервері:")
[print(topic) for topic in admin_client.list_topics() if MY_NAME in topic]

admin_client.close()
print("=" * 60)
print("  Готово".center(60))
print("=" * 60)
