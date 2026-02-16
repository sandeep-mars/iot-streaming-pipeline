from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# 1. Start the Spark Session
spark = SparkSession.builder \
    .appName("IoTStreamingPipeline") \
    .get_session()

# 2. Define what our "Sensor Data" looks like
schema = StructType() \
    .add("sensor_id", StringType()) \
    .add("temperature", DoubleType()) \
    .add("timestamp", TimestampType())

# 3. Read data from Kafka (the "Post Office" for data)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-data") \
    .load()

# 4. Clean the data and group it by 1-minute windows
clean_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# This calculates the average temperature every minute
averages = clean_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "1 minute"), col("sensor_id")) \
    .avg("temperature")

# 5. Save the results to Delta Lake
query = averages.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/mnt/checkpoints/iot_data") \
    .start("/mnt/delta/iot_averages")

query.awaitTermination()
