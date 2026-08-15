import os
import sys
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# DQ check field sets — single source of truth is src/dq.py (the live consumer
# applies the same checks to dicts); these constants keep the Spark column
# expressions in lockstep with it.
from src.dq import DQ_BALANCE_FIELDS, DQ_REQUIRED_FIELDS

# Must match the PySpark release AND its Scala build (2.13 for Spark 4.x).
SPARK_VERSION = "4.2.0"

# 1. Define PaySim Transaction Schema
PAYSIM_SCHEMA = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
])


def s3_config():
    """S3 sink settings, or None to keep writing Bronze locally.

    STREAMGUARD_S3_PROVIDER selects the environment:
      - "minio" (default): local mock - endpoint + path-style access + static
        keys, proved in SUGGESTIONS #2.
      - "aws": production - no endpoint (regional default), virtual-hosted
        style, HTTPS, and DefaultAWSCredentialsProviderChain (reads
        ~/.aws/credentials locally; IAM role if Spark runs on EC2/EKS).
    """
    bucket = os.getenv("STREAMGUARD_S3_BUCKET")
    if not bucket:
        return None
    provider = os.getenv("STREAMGUARD_S3_PROVIDER", "minio").strip().lower()
    if provider not in ("minio", "aws"):
        raise ValueError(f"STREAMGUARD_S3_PROVIDER must be 'minio' or 'aws', got {provider!r}")
    cfg = {"bucket": bucket, "provider": provider}
    if provider == "minio":
        cfg.update({
            "endpoint": os.getenv("STREAMGUARD_S3_ENDPOINT", "http://localhost:9000"),
            "access_key": os.getenv("STREAMGUARD_S3_ACCESS_KEY", "minioadmin"),
            "secret_key": os.getenv("STREAMGUARD_S3_SECRET_KEY", "minioadmin"),
        })
    return cfg


def build_spark():
    builder = SparkSession.builder \
        .appName("StreamGuard-Bronze-Consumer") \
        .config("spark.sql.shuffle.partitions", "2")

    packages = [f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}"]
    if s3_config():
        # Brings in hadoop-aws + aws-java-sdk-bundle so S3A works (the _2.12
        # connector stops at Spark 3.5; the _2.13 line matches Spark 4.x).
        packages.append(f"org.apache.spark:spark-hadoop-cloud_2.13:{SPARK_VERSION}")

    return builder.config("spark.jars.packages", ",".join(packages)).getOrCreate()


def apply_s3_hadoop_conf(spark):
    cfg = s3_config()
    hc = spark.sparkContext._jsc.hadoopConfiguration()
    if cfg["provider"] == "minio":
        hc.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hc.set("fs.s3a.endpoint", cfg["endpoint"])
        hc.set("fs.s3a.access.key", cfg["access_key"])
        hc.set("fs.s3a.secret.key", cfg["secret_key"])
        hc.set("fs.s3a.path.style.access", "true")
        hc.set("fs.s3a.connection.ssl.enabled", "false")
    else:
        hc.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hc.set("fs.s3a.path.style.access", "false")
        hc.set("fs.s3a.connection.ssl.enabled", "true")
        hc.set(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )


def sink_paths():
    cfg = s3_config()
    if cfg:
        root = f"s3a://{cfg['bucket']}"
        return f"{root}/transactions", f"{root}/checkpoints/bronze_transactions", \
            f"{root}/transactions_dq_rejected", f"{root}/checkpoints/bronze_dq_rejected"
    return "./data/bronze/transactions", "./data/checkpoints/bronze_transactions", \
        "./data/bronze/transactions_dq_rejected", "./data/checkpoints/bronze_dq_rejected"


def dq_rejected_condition():
    """P2-2: light Bronze-layer DQ filter, mirroring src.dq.dq_reject_reasons.

    Rejects (a) rows with missing required fields or whose JSON failed to parse
    into the schema (from_json yields NULL columns), (b) negative amounts, and
    (c) negative balances. Non-exhaustive by design — see src/dq.py.
    """
    null_fields = reduce(
        lambda acc, f: acc | col(f).isNull(), DQ_REQUIRED_FIELDS, lit(False)
    )
    negative_balance = reduce(
        lambda acc, f: acc | (col(f) < 0), DQ_BALANCE_FIELDS, lit(False)
    )
    return null_fields | (col("amount") < 0) | negative_balance


def start_streaming_job():
    print("Starting PySpark Structured Streaming consumer...")

    # 2. Initialize Spark Session with the Kafka package (and, in S3 mode,
    #    the S3A/hadoop-cloud jars).
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    cfg = s3_config()
    if cfg:
        apply_s3_hadoop_conf(spark)
        print(f"S3 sink enabled ({cfg['provider']}) -> s3a://{cfg['bucket']}/transactions")

    # 3. Read Stream from Redpanda / Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:19092") \
        .option("subscribe", "transactions-raw") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "1000000") \
        .load()

    # 4. Parse JSON Payload and Add Metadata
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), PAYSIM_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("ingested_at", current_timestamp())

    # 5. Sink Stream to Bronze Parquet (S3/minIO, or local data/bronze).
    #    P2-2: DQ-passed rows go to transactions/ (type-partitioned, as always);
    #    rejected rows are QUARANTINED to transactions_dq_rejected/ (kept for
    #    inspection, never merged into Bronze). Two independent streams, each
    #    with its own checkpoint so both consume every micro-batch exactly once.
    output_dir, checkpoint_dir, rejected_dir, rejected_checkpoint = sink_paths()
    rejected = dq_rejected_condition()

    # processingTime trigger buffers events into one micro-batch per interval,
    # producing fewer, larger parquet files instead of one tiny file per event
    # (important for S3 request costs and Athena query performance).
    parsed_stream.filter(~rejected).writeStream \
        .format("parquet") \
        .option("checkpointLocation", checkpoint_dir) \
        .option("path", output_dir) \
        .partitionBy("type") \
        .trigger(processingTime="30 seconds") \
        .outputMode("append") \
        .start()

    parsed_stream.filter(rejected).writeStream \
        .format("parquet") \
        .option("checkpointLocation", rejected_checkpoint) \
        .option("path", rejected_dir) \
        .partitionBy("type") \
        .trigger(processingTime="30 seconds") \
        .outputMode("append") \
        .start()

    print(f"Stream consumer running! Writing Parquet to '{output_dir}' "
          f"(DQ-rejected -> '{rejected_dir}')...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    try:
        start_streaming_job()
    except KeyboardInterrupt:
        print("\nStopping PySpark consumer...")
        sys.exit(0)
