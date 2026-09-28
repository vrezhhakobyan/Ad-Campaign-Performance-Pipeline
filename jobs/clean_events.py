"""Job 1: read one day of raw CSV, clean it, write to the Iceberg events table.

Also saves the bad-row ratio to a small file so the quality-check task can read it.
"""
import argparse
import json
import os

from pyspark.sql.types import (DoubleType, StringType, StructField, StructType)

from jobs import config
from jobs.transformations import bad_row_ratio, clean_events

RAW_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("campaign_id", StringType()),
    StructField("ad_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("cost", DoubleType()),
    StructField("country", StringType()),
    StructField("device", StringType()),
])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    args = p.parse_args()

    spark = config.build_spark("clean_events")
    spark.sparkContext.setLogLevel("WARN")

    raw = spark.read.option("header", True).schema(RAW_SCHEMA).csv(config.raw_path(args.date))
    raw = raw.cache()
    clean = clean_events(raw).cache()

    ratio = bad_row_ratio(raw, clean)
    print(f"[clean_events] raw={raw.count()} clean={clean.count()} bad_ratio={ratio:.4f}")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.CATALOG}.{config.DATABASE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.EVENTS_TABLE} (
            event_id STRING, event_time TIMESTAMP, campaign_id STRING, ad_id STRING,
            user_id STRING, event_type STRING, cost DOUBLE, country STRING,
            device STRING, event_date DATE
        ) USING iceberg
        PARTITIONED BY (event_date)
    """)

    # Replace only the partition(s) for this day, so re-running a day is safe.
    (clean.select("event_id", "event_time", "campaign_id", "ad_id", "user_id",
                  "event_type", "cost", "country", "device", "event_date")
          .writeTo(config.EVENTS_TABLE).overwritePartitions())

    os.makedirs("data/quality", exist_ok=True)
    with open(f"data/quality/{args.date}.json", "w") as f:
        json.dump({"date": args.date, "bad_row_ratio": ratio}, f)

    spark.stop()


if __name__ == "__main__":
    main()
