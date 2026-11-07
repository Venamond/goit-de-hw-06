import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, from_json, window, avg, current_timestamp,
                                   date_format, to_json, struct)
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from configs import spark_kafka_options, SENSORS_TOPIC, ALERTS_TOPIC

# Spark працює в JVM: вказуємо Java 21, встановлену через brew
os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@21")
# Конектор Kafka для Spark 4.x (Scala 2.13)
os.environ["PYSPARK_SUBMIT_ARGS"] = \
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 pyspark-shell"

spark = (SparkSession.builder
         .appName("SensorAlerts")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. Читання потоку з топіку сенсорів
raw_df = (spark.readStream
          .format("kafka")
          .options(**spark_kafka_options())
          .option("subscribe", SENSORS_TOPIC)
          .option("startingOffsets", "latest")
          .load())

json_schema = StructType([
    StructField("id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", StringType()),
])

sensors_df = (raw_df
              .select(from_json(col("value").cast("string"), json_schema).alias("data"))
              .select("data.*")
              .withColumn("timestamp", col("timestamp").cast("timestamp")))

# 2. Sliding window 1 хв / крок 30 с, watermark 10 с -> середні значення
avg_df = (sensors_df
          .withWatermark("timestamp", "10 seconds")
          .groupBy(window("timestamp", "1 minute", "30 seconds"))
          .agg(avg("temperature").alias("t_avg"),
               avg("humidity").alias("h_avg")))

# 3. Умови алертів читаємо з файлу, щоб змінювати їх без зміни коду
conditions_df = spark.read.csv("alerts_conditions.csv", header=True, inferSchema=True)

# 4. Cross join + фільтрація; -999 означає, що межа не перевіряється
alerts_df = (avg_df.crossJoin(conditions_df)
             .filter(((col("temperature_min") == -999) | (col("t_avg") >= col("temperature_min"))) &
                     ((col("temperature_max") == -999) | (col("t_avg") <= col("temperature_max"))) &
                     ((col("humidity_min") == -999) | (col("h_avg") >= col("humidity_min"))) &
                     ((col("humidity_max") == -999) | (col("h_avg") <= col("humidity_max"))))
             .select("window", "t_avg", "h_avg",
                     col("code").cast("string").alias("code"),
                     "message",
                     date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("timestamp")))

# 5a. Вивід алертів у консоль - для наочності
console_query = (alerts_df.writeStream
                 .trigger(processingTime="10 seconds")
                 .outputMode("append")
                 .format("console")
                 .option("truncate", False)
                 .start())

# 5b. Запис алертів у вихідний Kafka-топік у форматі JSON
kafka_query = (alerts_df
               .select(to_json(struct("*")).alias("value"))
               .writeStream
               .trigger(processingTime="10 seconds")
               .outputMode("append")
               .format("kafka")
               .options(**spark_kafka_options())
               .option("topic", ALERTS_TOPIC)
               .option("checkpointLocation", "checkpoints/alerts")
               .start())

spark.streams.awaitAnyTermination()
