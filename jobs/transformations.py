"""Pure Spark transformations (no reading or writing), so they are easy to test."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_EVENT_TYPES = ["impression", "click"]


def clean_events(raw: DataFrame) -> DataFrame:
    """Turn messy raw events into a clean table.

    Steps:
      1. Fix casing/spaces in event_type and campaign_id.
      2. Parse the timestamp and make an event_date column.
      3. Drop rows that are unusable (no campaign, bad type, negative cost, bad time).
      4. Remove duplicate events (same event_id).
    """
    df = (
        raw.withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .withColumn("campaign_id", F.trim(F.col("campaign_id")))
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("cost", F.col("cost").cast("double"))
    )
    df = df.withColumn("event_date", F.to_date("event_time"))

    valid = (
        F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("campaign_id").isNotNull()
        & (F.col("campaign_id") != "")
        & F.col("event_type").isin(VALID_EVENT_TYPES)
        & F.col("cost").isNotNull()
        & (F.col("cost") >= 0)
    )
    return df.filter(valid).dropDuplicates(["event_id"])


def bad_row_ratio(raw: DataFrame, clean: DataFrame) -> float:
    """Share of raw rows that were dropped (invalid rows plus duplicates)."""
    raw_count = raw.count()
    if raw_count == 0:
        return 1.0
    return (raw_count - clean.count()) / raw_count


def build_campaign_metrics(events: DataFrame) -> DataFrame:
    """One row per campaign per day with the numbers a marketing team looks at.

    ctr = clicks / impressions
    cpc = spend / clicks
    cpm = spend per 1000 impressions
    """
    agg = events.groupBy("event_date", "campaign_id").agg(
        F.sum(F.when(F.col("event_type") == "impression", 1).otherwise(0)).alias("impressions"),
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
        F.round(F.sum("cost"), 4).alias("spend"),
        F.countDistinct("user_id").alias("unique_users"),
    )
    return (
        agg.withColumn(
            "ctr",
            F.when(F.col("impressions") > 0,
                   F.round(F.col("clicks") / F.col("impressions"), 5)),
        )
        .withColumn(
            "cpc",
            F.when(F.col("clicks") > 0, F.round(F.col("spend") / F.col("clicks"), 4)),
        )
        .withColumn(
            "cpm",
            F.when(F.col("impressions") > 0,
                   F.round(F.col("spend") / F.col("impressions") * 1000, 4)),
        )
        .select("event_date", "campaign_id", "impressions", "clicks",
                "ctr", "spend", "cpc", "cpm", "unique_users")
    )
