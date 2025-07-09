from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# 1. Spark Session
spark = SparkSession.builder \
    .appName("FacebookAdsRealTimeProcessing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define Facebook Ads schema (simplified example)
fb_schema = StructType() \
    .add("ad_id", StringType()) \
    .add("campaign_id", StringType()) \
    .add("impressions", DoubleType()) \
    .add("clicks", DoubleType()) \
    .add("spend", DoubleType()) \
    .add("timestamp", TimestampType())

# 3. Read data from Kafka
ads_raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "facebook_ads_topic") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Convert binary Kafka 'value' to JSON string
ads_df = ads_raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), fb_schema).alias("data")) \
    .select("data.*")

# 5. Add derived metrics (CTR etc.)
ads_transformed_df = ads_df.withColumn("CTR", col("clicks") / col("impressions"))

# 6. Write to console/sink (for testing — replace with S3, Delta Lake, Redshift etc.)
query = ads_transformed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()