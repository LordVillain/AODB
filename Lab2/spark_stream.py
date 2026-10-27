import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, session_window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark_stream.py <window_type>")
        sys.exit(1)

    window_type = sys.argv[1]

    spark = SparkSession.builder.appName("SparkKafkaStream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-lab2:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "latest") \
        .load()

    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("temp", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    json_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    watermarked_df = json_df.withWatermark("timestamp", "10 seconds")

    if window_type == "tumbling":
        agg_df = watermarked_df.groupBy(
            window(col("timestamp"), "1 minute"), col("sensor_id")
        ).count()
        out_mode = "update"
        
    elif window_type == "sliding":
        agg_df = watermarked_df.groupBy(
            window(col("timestamp"), "1 minute", "30 seconds"), col("sensor_id")
        ).count()
        out_mode = "update"
        
    elif window_type == "session":
        agg_df = watermarked_df.groupBy(
            session_window(col("timestamp"), "30 seconds"), col("sensor_id")
        ).count()
        out_mode = "complete"
        
    else:
        print("Unknown window type!")
        sys.exit(1)

    query = agg_df.writeStream \
        .outputMode(out_mode) \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()