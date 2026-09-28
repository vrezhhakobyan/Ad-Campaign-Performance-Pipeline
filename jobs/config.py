"""Shared settings and the Spark session builder.

Two modes, chosen with the PIPELINE_MODE environment variable:
  local -> files and Iceberg tables live in ./data and ./warehouse
  aws   -> raw files and Iceberg tables live in S3, catalog is AWS Glue
"""
import os

from pyspark.sql import SparkSession

MODE = os.getenv("PIPELINE_MODE", "local")
S3_BUCKET = os.getenv("S3_BUCKET", "")
AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")

ICEBERG_PACKAGES = ["org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"]
if MODE == "aws":
    ICEBERG_PACKAGES += [
        "org.apache.iceberg:iceberg-aws-bundle:1.5.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ]

CATALOG = "ads"            # Spark catalog name
DATABASE = "ad_analytics"  # Iceberg database (a Glue database in AWS mode)

EVENTS_TABLE = f"{CATALOG}.{DATABASE}.events_clean"
METRICS_TABLE = f"{CATALOG}.{DATABASE}.campaign_daily_metrics"

# Fail the pipeline if more than this share of raw rows is unusable.
MAX_BAD_ROW_RATIO = float(os.getenv("MAX_BAD_ROW_RATIO", "0.05"))


def raw_path(event_date: str) -> str:
    """Where the raw CSV for one day lives."""
    if MODE == "aws":
        return f"s3a://{S3_BUCKET}/raw/events/event_date={event_date}/"
    return f"data/raw/events/event_date={event_date}/"


def build_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ",".join(ICEBERG_PACKAGES))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if MODE == "aws":
        builder = (
            builder.config(
                f"spark.sql.catalog.{CATALOG}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            )
            .config(f"spark.sql.catalog.{CATALOG}.warehouse", f"s3://{S3_BUCKET}/warehouse/")
            .config(
                f"spark.sql.catalog.{CATALOG}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
        )
    else:
        builder = (
            builder.config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
            .config(f"spark.sql.catalog.{CATALOG}.warehouse", "warehouse")
        )
    return builder.getOrCreate()
