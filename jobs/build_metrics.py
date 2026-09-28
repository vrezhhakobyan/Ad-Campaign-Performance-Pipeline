"""Job 3: build daily campaign metrics from the clean events table."""
import argparse

from pyspark.sql import functions as F

from jobs import config
from jobs.transformations import build_campaign_metrics


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    args = p.parse_args()

    spark = config.build_spark("build_metrics")
    spark.sparkContext.setLogLevel("WARN")

    events = spark.table(config.EVENTS_TABLE).filter(F.col("event_date") == F.lit(args.date).cast("date"))
    metrics = build_campaign_metrics(events)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.METRICS_TABLE} (
            event_date DATE, campaign_id STRING, impressions BIGINT, clicks BIGINT,
            ctr DOUBLE, spend DOUBLE, cpc DOUBLE, cpm DOUBLE, unique_users BIGINT
        ) USING iceberg
        PARTITIONED BY (event_date)
    """)
    metrics.writeTo(config.METRICS_TABLE).overwritePartitions()

    print(f"[build_metrics] wrote {metrics.count()} campaign rows for {args.date}")
    metrics.orderBy("campaign_id").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
