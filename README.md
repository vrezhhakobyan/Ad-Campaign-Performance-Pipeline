# Ad Campaign Performance Pipeline

A small data engineering project: a daily pipeline that cleans raw ad events
(impressions and clicks) and builds campaign performance metrics
(click rate, cost per click, spend) that a marketing team can query.

**Stack:** Python · PySpark · Apache Airflow · Apache Iceberg · AWS S3 · AWS Glue · Amazon Athena · Docker

## How it works

```
 generate_data  ->  clean_events  ->  quality_check  ->  build_metrics
 (raw CSV to S3)    (PySpark)         (fails if >5%      (PySpark)
                    Iceberg table      rows are bad)     Iceberg table
                    events_clean                         campaign_daily_metrics
                                                                |
                                                          Athena / SQL
```

1. **generate_data** creates one day of fake but realistic ad events. The data is
   deliberately messy: duplicates, missing campaign ids, negative costs, and
   inconsistent text like `" CLICK "`.
2. **clean_events** (PySpark) fixes the text, drops unusable rows, removes
   duplicates, and writes an Iceberg table partitioned by day.
3. **quality_check** stops the pipeline if too many raw rows were bad
   (default limit: 5%). This protects the reporting table from bad data.
4. **build_metrics** (PySpark) computes per campaign per day: impressions,
   clicks, CTR, spend, cost per click (CPC), cost per 1000 impressions (CPM),
   and unique users.

Airflow runs the four steps every day. Each write replaces only that day's
partition, so **re-running a day is safe** (no duplicate data).

## Project layout

```
dags/ad_pipeline_dag.py        Airflow DAG (the 4 steps above)
jobs/transformations.py        Spark logic (pure functions, unit tested)
jobs/clean_events.py           Job 1
jobs/quality_check.py          Job 2
jobs/build_metrics.py          Job 3
jobs/config.py                 Settings + Spark session (local or AWS)
data_generator/generate_data.py  Fake data with deliberate problems
athena/queries.sql             Example Athena queries + Iceberg time travel
tests/                         Unit tests for the Spark logic
docker/, docker-compose.yml    Airflow with Java + PySpark
```

## Run it locally (no AWS needed)

Needs: Python 3.10+, Java 17+ (Spark needs it), and internet on first run
(Spark downloads the Iceberg library).

```bash
pip install -r requirements.txt
make test          # unit tests
make run-local     # generate data -> clean -> quality check -> metrics
```

Iceberg tables are created in `./warehouse`.

### With Airflow (Docker)

```bash
make airflow
```

Open http://localhost:8080. The admin password is printed in the container
logs on first start. Turn on the `ad_campaign_pipeline` DAG and trigger it.

## Run it on AWS (S3 + Glue + Athena)

1. Create an S3 bucket and set up AWS credentials on your machine.
2. Copy `.env.example` to `.env`, then set `PIPELINE_MODE=aws` and `S3_BUCKET=<your bucket>`.
3. Run the pipeline the same way. Raw files go to `s3://<bucket>/raw/`,
   Iceberg tables go to `s3://<bucket>/warehouse/`, and the tables are
   registered in the AWS Glue Data Catalog automatically.
4. Open Amazon Athena, pick the `ad_analytics` database, and run
   the queries in `athena/queries.sql`.

Your AWS user/role needs permission for S3 (read/write on the bucket) and
Glue (create/read databases and tables), plus Athena to run queries.

**Cost:** at this data size it should cost a few cents. Delete the bucket
when you are done.

## Design choices

- **Iceberg** instead of plain Parquet: it supports safe partition overwrites,
  table history, and time travel (see queries 5 and 6 in `athena/queries.sql`).
- **Pure transformation functions** are separated from reading/writing, so they
  can be tested without a cluster.
- **Data quality gate** in the DAG: bad data fails loudly instead of silently
  producing wrong numbers.
- **Idempotent daily runs**: dynamic partition overwrite means retries and
  backfills do not create duplicates.

## Ideas for next steps

- Move Spark to AWS EMR or Glue jobs instead of running in the Airflow container
- Add a Kafka stream for real-time events
- Add more quality checks (null rates, freshness, row count drops)
- Deploy the infrastructure with CloudFormation or Terraform
- Add a CI pipeline (GitHub Actions) that runs the tests on every push
