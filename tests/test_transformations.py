from datetime import date

import pytest
from pyspark.sql import SparkSession

from jobs.transformations import bad_row_ratio, build_campaign_metrics, clean_events

COLS = ["event_id", "event_time", "campaign_id", "ad_id", "user_id",
        "event_type", "cost", "country", "device"]


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield s
    s.stop()


def raw_df(spark, rows):
    return spark.createDataFrame(rows, COLS)


def test_clean_events_removes_bad_rows(spark):
    rows = [
        ("1", "2026-09-01T10:00:00", "cmp_a", "ad1", "u1", "impression", 0.002, "US", "mobile"),
        ("1", "2026-09-01T10:00:00", "cmp_a", "ad1", "u1", "impression", 0.002, "US", "mobile"),  # duplicate
        ("2", "2026-09-01T10:00:05", "",      "ad1", "u1", "click",      0.0,   "US", "mobile"),  # no campaign
        ("3", "2026-09-01T10:00:09", "cmp_a", "ad1", "u2", "impression", -0.5,  "US", "mobile"),  # negative cost
        ("4", "2026-09-01T10:01:00", "cmp_a", "ad1", "u2", " CLICK ",    0.0,   "US", "mobile"),  # messy casing, valid
        ("5", "2026-09-01T10:01:00", "cmp_a", "ad1", "u2", "purchase",   0.0,   "US", "mobile"),  # unknown type
    ]
    raw = raw_df(spark, rows)
    clean = clean_events(raw)

    assert sorted(r.event_id for r in clean.collect()) == ["1", "4"]
    assert {r.event_type for r in clean.collect()} == {"impression", "click"}
    assert bad_row_ratio(raw, clean) == pytest.approx(4 / 6)


def test_campaign_metrics_math(spark):
    rows = []
    for i in range(10):  # 10 impressions, cost 0.004 each -> spend 0.04
        rows.append((f"i{i}", "2026-09-01T10:00:00", "cmp_a", "ad", f"u{i}", "impression", 0.004, "US", "mobile"))
    for i in range(2):   # 2 clicks
        rows.append((f"c{i}", "2026-09-01T10:00:30", "cmp_a", "ad", f"u{i}", "click", 0.0, "US", "mobile"))

    metrics = build_campaign_metrics(clean_events(raw_df(spark, rows))).collect()
    assert len(metrics) == 1
    m = metrics[0]
    assert m.event_date == date(2026, 9, 1)
    assert m.impressions == 10 and m.clicks == 2
    assert m.ctr == pytest.approx(0.2)
    assert m.spend == pytest.approx(0.04)
    assert m.cpc == pytest.approx(0.02)
    assert m.cpm == pytest.approx(4.0)
    assert m.unique_users == 10


def test_metrics_no_clicks_gives_null_cpc(spark):
    rows = [("i1", "2026-09-01T10:00:00", "cmp_b", "ad", "u1", "impression", 0.003, "US", "mobile")]
    m = build_campaign_metrics(clean_events(raw_df(spark, rows))).collect()[0]
    assert m.clicks == 0 and m.cpc is None
