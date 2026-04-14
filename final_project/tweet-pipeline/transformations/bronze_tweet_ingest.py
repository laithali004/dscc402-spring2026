# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Raw Tweet Ingestion
# MAGIC
# MAGIC ## Purpose
# MAGIC Ingest raw JSON tweets from S3 using CloudFiles Auto Loader without transformation.
# MAGIC Preserve original data structure and add metadata for lineage tracking.
# MAGIC
# MAGIC ## Requirements
# MAGIC - Use Spark Declarative Pipelines API (`pyspark.pipelines`)
# MAGIC - Configure CloudFiles for incremental ingestion
# MAGIC - Enforce explicit schema for data quality
# MAGIC - Add metadata columns: source_file, processing_time
# MAGIC
# MAGIC ## Expected Output
# MAGIC Delta table: `tweets_bronze`
# MAGIC - ~50,000 rows ingested from S3
# MAGIC - All source JSON fields preserved
# MAGIC - Metadata columns populated for all rows
# MAGIC
# MAGIC ## Reference
# MAGIC See Lab 0.4 (Delta Lake) for CloudFiles Auto Loader patterns

# COMMAND ----------

# DBTITLE 1,Cell 2
# TODO: Import necessary libraries
# You will need:
# - pyspark.pipelines (as dp)
# - pyspark.sql.types (for schema definition)
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import *
from pyspark import pipelines as dp

# COMMAND ----------

# Enable legacy time parser for Twitter datetime format
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Create Bronze Streaming Table
# MAGIC
# MAGIC TODO: Use dp.create_streaming_table() to define the target table.
# MAGIC - Table name: "tweets_bronze"
# MAGIC - Add a descriptive comment

# COMMAND ----------

# DBTITLE 1,Cell 5
# Define source schema (JSON files)
tweet_source_schema = StructType([
    StructField("date", StringType(), True),
    StructField("sentiment", StringType(), True),
    StructField("text", StringType(), True),
    StructField("user", StringType(), True),
])

# Define table schema (includes metadata columns)
tweet_schema = StructType([
    StructField("date", StringType(), True),
    StructField("sentiment", StringType(), True),
    StructField("text", StringType(), True),
    StructField("user", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("processing_time", TimestampType(), True),
])

dp.create_streaming_table(
    name="tweets_bronze",
    comment="Bronze streaming table for raw tweet records",
    schema=tweet_schema
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Define Tweet Schema
# MAGIC
# MAGIC TODO: Define explicit StructType schema with these fields:
# MAGIC - date (StringType)
# MAGIC - user (StringType)
# MAGIC - text (StringType)
# MAGIC - sentiment (StringType)
# MAGIC
# MAGIC CloudFiles requires explicit schemas for JSON data quality enforcement.

# COMMAND ----------

# TODO: Define tweet schema as StructType
tweet_schema = StructType([
    StructField("date", StringType(), True),
    StructField("sentiment", StringType(), True),
    StructField("text", StringType(), True),
    StructField("user", StringType(), True),
])



# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Define Ingestion Flow
# MAGIC
# MAGIC TODO: Create @dp.append_flow function that:
# MAGIC 1. Reads streaming data with CloudFiles (format: "cloudFiles")
# MAGIC 2. Configures JSON as the data format
# MAGIC 3. Sets schema checkpoint location: "/Volumes/workspace/default/checkpoints/"
# MAGIC 4. Applies the tweet schema defined above
# MAGIC 5. Loads from: "s3://dsas-datasets/tweets/"
# MAGIC 6. Adds metadata columns:
# MAGIC    - source_file from _metadata.file_path
# MAGIC    - processing_time using current_timestamp()
# MAGIC
# MAGIC Reference: Lab 0.4 Task 3.1 for CloudFiles configuration

# COMMAND ----------

# DBTITLE 1,Cell 9
# TODO: Define append_flow function for bronze ingestion
@dp.append_flow(target="tweets_bronze")
def tweets_bronze_flow():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(tweet_source_schema)
        .load("s3://dsas-datasets/tweets/")
        .select(
            "*",
            col("_metadata.file_path").cast("string").alias("source_file"),
            current_timestamp().alias("processing_time")
        )
    )
# from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC After pipeline execution, verify:
# MAGIC - Table exists: tweets_bronze
# MAGIC - Row count: ~50,000
# MAGIC - All JSON fields present: date, user, text, sentiment
# MAGIC - Metadata columns populated: source_file, processing_time
