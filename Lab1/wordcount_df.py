import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col

if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("WordCountDF").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.text(input_path)
    words = df.select(explode(split(col("value"), r"\s+")).alias("word"))
    counts = words.groupBy("word").count()

    counts.write.mode("overwrite").csv(output_path)
    spark.stop()