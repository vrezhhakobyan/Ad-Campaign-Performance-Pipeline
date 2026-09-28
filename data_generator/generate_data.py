"""Generate one day of fake ad events (impressions + clicks) as CSV.

The data is deliberately a bit messy (duplicates, missing campaign ids,
negative costs, inconsistent casing) so the cleaning step has real work to do.

Usage:
    python -m data_generator.generate_data --date 2026-09-01 --rows 50000
    python -m data_generator.generate_data --date 2026-09-01 --upload   # AWS mode
"""
import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta


CAMPAIGNS = {
    # campaign_id: (click-through rate, cost per 1000 impressions in USD)
    "cmp_search_brand": (0.060, 4.50),
    "cmp_search_generic": (0.035, 3.20),
    "cmp_social_video": (0.012, 1.80),
    "cmp_social_carousel": (0.018, 2.10),
    "cmp_display_retarget": (0.025, 2.60),
    "cmp_display_prospect": (0.006, 0.90),
}
DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.62, 0.32, 0.06]
COUNTRIES = ["US", "GB", "DE", "FR", "AM", "IN", "BR"]


def make_rows(event_date: str, n_impressions: int, seed: int = 42):
    rng = random.Random(f"{seed}-{event_date}")
    day_start = datetime.strptime(event_date, "%Y-%m-%d")
    rows = []

    for _ in range(n_impressions):
        campaign = rng.choice(list(CAMPAIGNS))
        ctr, cpm = CAMPAIGNS[campaign]
        ts = day_start + timedelta(seconds=rng.randint(0, 86399))
        user = f"user_{rng.randint(1, 20000)}"
        device = rng.choices(DEVICES, DEVICE_WEIGHTS)[0]
        country = rng.choice(COUNTRIES)
        ad_id = f"{campaign}_ad{rng.randint(1, 4)}"
        imp_id = str(uuid.UUID(int=rng.getrandbits(128)))

        rows.append([imp_id, ts.isoformat(), campaign, ad_id, user,
                     "impression", round(cpm / 1000, 6), country, device])

        if rng.random() < ctr:
            click_ts = ts + timedelta(seconds=rng.randint(1, 60))
            click_id = str(uuid.UUID(int=rng.getrandbits(128)))
            rows.append([click_id, click_ts.isoformat(), campaign, ad_id, user,
                         "click", 0.0, country, device])

    # ---- add messy rows -------------------------------------------------
    n = len(rows)
    for _ in range(int(n * 0.010)):          # exact duplicates
        rows.append(list(rng.choice(rows)))
    for r in rng.sample(rows, int(n * 0.004)):  # missing campaign id
        r[2] = ""
    for r in rng.sample(rows, int(n * 0.003)):  # negative cost
        r[6] = -abs(r[6]) if r[6] else -0.001
    for r in rng.sample(rows, int(n * 0.02)):   # inconsistent casing / spaces
        r[5] = rng.choice([r[5].upper(), f" {r[5]} ", r[5].capitalize()])

    rng.shuffle(rows)
    return rows


HEADER = ["event_id", "event_time", "campaign_id", "ad_id", "user_id",
          "event_type", "cost", "country", "device"]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--rows", type=int, default=50000, help="number of impressions")
    p.add_argument("--upload", action="store_true", help="upload to S3 (needs S3_BUCKET)")
    args = p.parse_args()

    out_dir = f"data/raw/events/event_date={args.date}"
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "events.csv")

    rows = make_rows(args.date, args.rows)
    with open(out_file, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        w.writerows(rows)
    print(f"Wrote {len(rows)} rows to {out_file}")

    if args.upload:
        import boto3  # only needed for AWS mode

        bucket = os.environ["S3_BUCKET"]
        key = f"raw/events/event_date={args.date}/events.csv"
        boto3.client("s3").upload_file(out_file, bucket, key)
        print(f"Uploaded to s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
