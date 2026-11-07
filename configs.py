kafka_config = {
    "bootstrap_servers": ['77.81.230.104:9092'],
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN',
}

# Префікс, щоб імена топіків не перетиналися з іншими студентами
MY_NAME = "olivin"

SENSORS_TOPIC = f"{MY_NAME}_building_sensors"
ALERTS_TOPIC = f"{MY_NAME}_spark_alerts"


def connection_params():
    # Спільні параметри підключення для producer / consumer / admin (kafka-python)
    return dict(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
    )


def spark_kafka_options():
    # Ті самі параметри підключення, але у форматі опцій Spark
    jaas = ('org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";')
    return {
        "kafka.bootstrap.servers": kafka_config['bootstrap_servers'][0],
        "kafka.security.protocol": kafka_config['security_protocol'],
        "kafka.sasl.mechanism": kafka_config['sasl_mechanism'],
        "kafka.sasl.jaas.config": jaas,
    }
