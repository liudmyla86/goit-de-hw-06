from configs import kafka_config

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType
)
from pyspark.sql.functions import (
    from_json, col, window, avg, to_timestamp, to_json, struct
)

# ---------------------------
# Spark Session
# ---------------------------
spark = (SparkSession.builder
    .appName("LiudmylaAlertsStream")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------
# Kafka configs
# ---------------------------
KAFKA_BOOTSTRAP = kafka_config["bootstrap_servers"][0]
INPUT_TOPIC = "building_sensors_liudmyla"
OUTPUT_TOPIC = "alerts_liudmyla"

# ---------------------------
# Sensor schema 
# ---------------------------
sensor_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
])

# ---------------------------
# Read Kafka stream (WITH SASL)
# ---------------------------
raw_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
    )
    .load()
)

parsed_df = (raw_df
    .selectExpr("CAST(value AS STRING) AS json_string")
    .select(from_json(col("json_string"), sensor_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp("timestamp"))
)

# ---------------------------
# Sliding window 1min / 30sec + 10sec watermark
# ---------------------------
agg_df = (parsed_df
    .withWatermark("event_time", "10 seconds")
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("sensor_id")
    )
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )
)

# ---------------------------
# Load alert conditions CSV
# ---------------------------
alerts_df = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("alerts_conditions.csv")
)

# ---------------------------
# Cross join + filtering
# ---------------------------
condition = (
    ((col("humidity_min") != -999) & (col("avg_humidity") < col("humidity_min"))) |
    ((col("humidity_max") != -999) & (col("avg_humidity") > col("humidity_max"))) |
    ((col("temperature_min") != -999) & (col("avg_temperature") < col("temperature_min"))) |
    ((col("temperature_max") != -999) & (col("avg_temperature") > col("temperature_max")))
)

alerts_stream = agg_df.crossJoin(alerts_df).where(condition)

# ---------------------------
# Prepare for Kafka output
# ---------------------------
result_df = alerts_stream.select(
    col("sensor_id"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("avg_temperature"),
    col("avg_humidity"),
    col("code").alias("alert_code"),
    col("message").alias("alert_message")
)

kafka_output = result_df.selectExpr(
    "CAST(sensor_id AS STRING) AS key",
    "to_json(struct(*)) AS value"
)

# ---------------------------
# Write alerts to Kafka (WITH SASL)
# ---------------------------
query = (kafka_output
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", OUTPUT_TOPIC)
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
    )
    .option("checkpointLocation", "/tmp/liudmyla_alerts_checkpoint")
    .outputMode("append")
    .start()
)

query.awaitTermination()
