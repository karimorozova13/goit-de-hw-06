import os
from configs import kafka_config
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

my_name = 'kariMo'
checkpoint_directory = os.path.expanduser("~/alerts")

building_sensors_topic = f'{my_name}_building_sensors'
alert_topic = f'{my_name}_alert_topic'

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

window_duration = "1 minute"
sliding_interval = "30 seconds"
watermark_duration = "10 seconds"

spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .getOrCreate())

df_alerts = spark.read.csv("alerts_conditions.csv", header=True)

# Read the stream from Kafka
df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
      .option("kafka.security.protocol", "SASL_PLAINTEXT")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.jaas.config",
              'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";')
      .option("subscribe", building_sensors_topic)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "500").load())

# Define schema for sensor data
json_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
])

# Process the sensor data
df_sensors = (
    df.selectExpr(
        "CAST(key AS STRING) AS key_deserialized",
        "CAST(value AS STRING) AS value_deserialized",
        "*",
    )
    .drop("key", "value")
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema))
    .withColumn("timestamp", when(col("value_json.timestamp").isNotNull(), (col("value_json.timestamp").cast("double").cast("timestamp"))).otherwise(None))
    .withColumn("temperature", col("value_json.temperature"))
    .withColumn("humidity", col("value_json.humidity"))
    .withColumn("sensor_id", col("value_json.sensor_id"))
    .drop("value_json", "value_deserialized")
)

# Data aggregation
df_avg = (
    df_sensors.withWatermark("timestamp", watermark_duration)
    .groupBy(window(col("timestamp"), window_duration, sliding_interval))
    .agg(
        avg("temperature").alias("t_avg"),
        avg("humidity").alias("h_avg"),
    )
    .select(
        col("window.start").alias("start"),
        col("window.end").alias("end"),
        col("t_avg"),
        col("h_avg"),
    )
)

# Apply alert conditions
true_alerts = (df_avg.crossJoin(df_alerts)
               .dropna()
               .where(
                   ((col("temperature_min") <= col("t_avg")) & (col("t_avg") <= col("temperature_max")) & (col("code") == "103")) |
                   ((col("temperature_min") <= col("t_avg")) & (col("t_avg") <= col("temperature_max")) & (col("code") == "104")) |
                   ((col("humidity_min") <= col("h_avg")) & (col("h_avg") <= col("humidity_max")) & (col("code") == "101")) |
                   ((col("humidity_min") <= col("h_avg")) & (col("h_avg") <= col("humidity_max")) & (col("code") == "102"))
               )
               .withColumn("timestamp", current_timestamp())
               .select("start", "end", "t_avg", "h_avg", "code", "message", "timestamp")
)

# Display the output in the console
true_alerts.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .start()

# Prepare data for Kafka output
prepare_to_kafka_df = true_alerts.select(
    to_json(struct((struct(col("start"), col("end")).alias("window")), col("t_avg"), col("h_avg"), col("code"), col("message"), col("timestamp"))).alias("value")
)

# Write to Kafka
try:
    kafka_query = (prepare_to_kafka_df.writeStream
                   .trigger(processingTime='10 seconds')
                   .format("kafka")
                   .option("kafka.bootstrap.servers", "77.81.230.104:9092")
                   .option("topic", alert_topic)
                   .option("kafka.security.protocol", "SASL_PLAINTEXT")
                   .option("kafka.sasl.mechanism", "PLAIN")
                   .option("kafka.sasl.jaas.config",
                           "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';")
                   .option("checkpointLocation", checkpoint_directory)
                   .start()
                   .awaitTermination())
except Exception as e:
    print(f"Error: {e}")
