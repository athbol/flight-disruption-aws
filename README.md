# flight-disruption-aws

A small airline runs 24 flights a day. Some are delayed, some are cancelled, and the passengers on a cancelled flight are rebooked or refunded. This repo generates those events as synthetic data, streams them through Kafka, keeps a live status table in DynamoDB, archives every event in S3, and once a day turns the archive into Parquet tables with a PySpark job on Glue, for querying in Athena. Everything on AWS is created by Pulumi from a GitHub Actions workflow. It is sized to run for under one dollar a month.

I built it to show, on a public repo with no customer data, how I would rebuild the disruption pipeline I worked on at an airline. It has the same problems: events arrive out of order, twice, or late. It keeps the same split between a live store for operations and an analytical store for reporting.

## How it fits together

![Architecture](docs/architecture.svg)

| Step | Runs on | What it shows |
|------|---------|---------------|
| A generator plans each day from a seed and emits booking, ticket and flight events at their scheduled times, with lag, duplicates and held cancellations mixed in | Docker on a VPS | A deterministic, replayable source that is messy in the ways real feeds are |
| Events sit on three Kafka topics with 48 hours of retention | Kafka 4.3 (KRaft), one broker, same VPS | Nothing on the AWS side needs to be up for the source to keep going |
| A consumer writes the newest state of each flight, booking and ticket | DynamoDB table `fda-live` | Writes are idempotent and safe against out-of-order delivery |
| The same consumer archives every event as gzip JSONL, partitioned by topic and arrival day, and commits Kafka offsets only after the archive write succeeds | S3 `raw/` | At-least-once delivery: a crash replays events instead of skipping them |
| A daily PySpark job on Glue, `fda-curate`, dedupes the archive, keeps the latest version of each entity, joins them into passenger journeys and rewrites the last four days as Parquet | Glue 5.0, 2 workers, Flex | Late and duplicate events are repaired without a backfill |
| Saved queries answer "which journeys were disrupted", "which flights were late yesterday" and "what happened to this passenger" | Athena, Glue catalog with partition projection | Analysts query it with plain SQL and nothing to keep running |

Pulumi describes all of it in `infra/`. A push to `main` runs `pulumi up` through GitHub Actions, which gets its AWS credentials from OIDC instead of stored keys. CloudWatch alarms and an EventBridge rule send an email when the consumer stops, when DynamoDB throttles, or when the daily job fails.

## How each part works

### The events

Every event is one JSON message. It has four common fields, the envelope, and the fields of its topic:

```json
{
  "event_id": "3f9c...",
  "event_type": "rebooked",
  "event_time": "2026-10-07T05:12:44+00:00",
  "sequence": 2,
  "booking_id": "B20261007-030",
  "passenger_id": "P20261007-030",
  "flight_id": "F0407-2026-10-07",
  "original_flight_id": "F0401-2026-10-07"
}
```

`src/fda/schemas.py` is the data contract. It lists the fields of each topic, the fields that are whole numbers, and the columns of the two curated tables. The generator, the consumer, the topic setup and the Spark job all import it. You add a new field in one file, and every part reads it. It is a shared module, not a schema registry. A registry becomes useful when another team or another language reads these topics.

The generator (`src/fda/generator.py`) works in four steps:

1. It plans 24 flights a day on six routes. Each flight is cancelled (8%), delayed by 15 to 240 minutes (12%) or on time.
2. It gives each flight 30 passengers, each with a booking and a ticket. On a cancelled flight, 70% of passengers are rebooked to another flight on the same route. The others are refunded.
3. It writes each change of a flight, booking or ticket as one event, with `sequence` 1, 2, 3 for that entity. For example, a booking is `created` (1), then `rebooked` (2).
4. It sends each event to Kafka at its due time. 85% go out within 5 seconds and 3% are up to 36 hours late. 2% go out twice. 30% of cancellations are held back for 1 to 3 hours.

All random choices come from a seed and the date. The same seed always gives the same day, so a run can be replayed exactly.

### Kafka

There are three topics: `flights`, `bookings` and `tickets`. Each has one partition. The message key is the flight, booking or ticket id, so all events of one entity stay in order. One partition is enough because one consumer handles about 1,400 passengers a day with no lag. More partitions would allow more consumers in parallel, but order would then hold only per key.

The broker keeps messages for 48 hours (`compose.yaml`). This retention is a buffer for a consumer outage. It is not the archive. The S3 `raw/` prefix is the archive.

### The consumer

`src/fda/consumer.py` runs in Docker next to Kafka. It reads all three topics as the group `fda-consumer`, with auto-commit off. For each message it does three things:

1. It writes the event to DynamoDB at once (`src/fda/live.py`).
2. It adds the raw message to a buffer in memory.
3. It counts the result per topic as written, stale or rejected.

The DynamoDB write is a conditional put: write only if the item does not exist, or if the stored `sequence` is lower. A duplicate or an older event fails the condition, and the consumer counts it as stale. The consumer stores bookings and tickets under `PAX#<passenger>`, so one query returns all of a passenger's items.

When the buffer reaches 5,000 events, 16 MB or 5 minutes, the consumer writes it to S3. Then it commits the Kafka offset. A crash before the commit replays those events, so no event is lost. Every 60 seconds the consumer sends a heartbeat and its counts to CloudWatch. An alarm sends an email when the heartbeat is missing for 10 minutes.

### The S3 bucket

One bucket holds four prefixes:

| Prefix | Content | Written by |
|--------|---------|------------|
| `raw/` | every event as received, gzip JSON lines | the consumer |
| `curated/` | the `journeys` and `flights` Parquet tables | the daily job |
| `artifacts/glue/` | the job's entry script and the `fda` package as a zip | Pulumi, on each deploy |
| `athena-results/` | the result of each Athena query | Athena |

Each IAM identity can write only to its own prefix. A raw file name holds the topic, the arrival day, the partition and the first and last offset:

```
raw/topic=bookings/dt=2026-10-07/0-000000001200-000000001450.jsonl.gz
```

The file holds each message unchanged on one line, duplicates included. The raw archive is the source of truth. If the daily job has a bug, a fix and a rerun rebuild the curated tables from it. If the consumer replays a batch, it writes the same file name with the same bytes, so the upload replaces the file.

### The daily job

`fda-curate` is a Glue job. Glue runs a PySpark script on workers that AWS starts for the run and stops after it. A Glue trigger starts the job at 02:30 UTC. The logic is in `src/fda/curate.py`, and `jobs/glue_curate.py` only starts it. On each deploy, Pulumi uploads both to `artifacts/glue/`, because Glue reads code from S3 only.

The job does six steps:

1. It selects the run day and the three days before it.
2. It reads those days of `raw/` and parses each line with the schema from `schemas.py`.
3. It drops events without an `event_id` or a `sequence`, and drops duplicates by `event_id`.
4. It keeps the event with the highest `sequence` per flight, booking and ticket. This is the same rule as the DynamoDB write, so the two stores agree.
5. It joins each booking to its current flight, its original flight and its ticket, and labels the disruption: `cancelled_rebooked`, `cancelled_refunded`, `delayed` or `none`.
6. It writes `journeys` and `flights` as Parquet, partitioned by `flight_date`. It overwrites only the four days it read, so a rerun gives the same result.

### Athena and DynamoDB

The two stores answer different questions:

| | DynamoDB `fda-live` | Athena on `curated/` |
|---|---|---|
| Question | What is the state of this passenger now? | How many journeys were disrupted last week? |
| Holds | The latest version of each entity | Every day, from the daily job |
| Access | One key, in milliseconds | SQL with `GROUP BY` and joins, in seconds |
| Fresh | At once | After the next daily run |

Athena runs SQL on the Parquet files in S3. It finds the tables in the Glue Data Catalog. Partition projection lets it find each new `flight_date` folder with no crawler. A query that filters on `flight_date` reads only those folders, and Parquet lets it read only the columns it needs.

## It runs

Screenshots from the live system, in the order the data flows.

<table>
<tr>
<td width="50%" valign="top">1. <code>docker compose ps</code> on the VPS: generator, consumer and Kafka up, Kafka healthy.<br><img src="docs/screenshots/01-compose-up.png" alt="docker compose ps on the VPS"></td>
<td width="50%" valign="top">2. One booking in the live table, keyed by passenger and booking, with its sequence number.<br><img src="docs/screenshots/02-dynamodb-item.png" alt="A DynamoDB item"></td>
</tr>
<tr>
<td width="50%" valign="top">3. Raw archive, one object per flush.<br><img src="docs/screenshots/03-s3-raw.png" alt="S3 raw prefix"></td>
<td width="50%" valign="top">4. The daily job, succeeded.<br><img src="docs/screenshots/04-glue-run.png" alt="Glue job run"></td>
</tr>
<tr>
<td width="50%" valign="top">5. The curated <code>flights</code> table queried in Athena, a few hours into the first day.<br><img src="docs/screenshots/05-athena-query.png" alt="Athena query"></td>
<td width="50%" valign="top">6. The <code>fda</code> dashboard: heartbeat, events written per topic, stale and rejected events (none yet) and DynamoDB write capacity.<br><img src="docs/screenshots/06-cloudwatch-dashboard.png" alt="CloudWatch dashboard"></td>
</tr>
<!-- 07-alarm-email.png is added with the passenger example on 2026-10-07 -->
<tr>
<td width="50%" valign="top">8. One CI run on main: the tests, then the deploy job.<br><img src="docs/screenshots/08-github-actions-green.png" alt="GitHub Actions"></td>
<td width="50%" valign="top">9. The Pulumi step of that deploy: one resource updated, one replaced, 47 unchanged.<br><img src="docs/screenshots/09-pulumi-up.png" alt="Pulumi up in CI"></td>
</tr>
</table>

## One passenger, two stores

The same synthetic passenger as the consumer sees them in DynamoDB and as an analyst sees them in Athena after the daily job. The values below show the shape of the two records. A real rebooked passenger replaces them after the first full-day run on 2026-10-07.

The DynamoDB item is the latest event for that booking, keyed `PAX#<passenger>` / `BOOKING#<booking>`, so one query by passenger returns their bookings and tickets:

```json
{ "pk": "PAX#...", "sk": "BOOKING#...", "event_type": "rebooked", "sequence": 2, "flight_id": "...", "original_flight_id": "..." }
```

The Athena row, from `sql/passenger_journey.sql`, joins that booking with both flights and the ticket, and classifies the journey:

```
passenger_id | flight_id | flight_status | booking_status | original_flight_id | original_flight_status | ticket_status | disruption
P...         | F0...     | departed      | rebooked       | F0...              | cancelled              | exchanged     | cancelled_rebooked
```

## The hard parts

Events arrive out of order. Every event carries a per-entity `sequence`. The consumer writes with a conditional put that only succeeds if the stored sequence is lower, so an old event that arrives late cannot overwrite a newer state. It counts the rejected write as `EventsStale` and moves on.

Events arrive twice. Event ids are UUIDv5 of seed, entity and sequence, so a re-sent event has the same id as the original. The conditional put drops it in DynamoDB. The daily job drops it again with `dropDuplicates("event_id")` before it picks the latest version of each entity.

Events arrive late. The generator delays some events by up to 36 hours and holds some cancellations for a few hours. The daily job therefore reads the last four days of the archive, rebuilds journeys for flights in that window, and overwrites only those date partitions. A flight that got its last event two days after it flew ends up correct on the next run, with no manual backfill. The archive is partitioned by the day an event arrived and the curated tables by flight date, so a late event lands in today's folder and still rewrites its flight's day. A flight older than the window is skipped, so a late event never overwrites a finished day with partial data.

The consumer commits Kafka offsets only after the S3 upload succeeds. It buffers events and uploads them to S3 every 5000 records, every 16 MB or every five minutes, whichever comes first. A crash before the commit replays everything since the last commit, up to five minutes of events, and the two rules above absorb the replay. The archive can therefore hold duplicate lines.

Each step has its own narrow IAM identity. The consumer has one IAM user that can write to one table, one S3 prefix and one metric namespace, and only from the VPS address. The daily job has a role that reads `raw/` and writes `curated/`. GitHub Actions assumes a deploy role through OIDC on pushes to `main` and a read-only role on pull requests. Every role or user the deploy role creates must carry a permissions boundary that has no IAM or STS rights, so CI cannot escalate through a new identity.

The account has a 1 USD monthly budget. It emails at 80% of actual spend and at 100% of forecast spend. The Athena workgroup cancels any query that scans more than 100 MB. Raw objects expire after 30 days.

## Run it yourself

The tests need [uv](https://docs.astral.sh/uv/) and Java 17; uv installs Python 3.11 itself.

```sh
git clone https://github.com/athbol/flight-disruption-aws && cd flight-disruption-aws
uv sync --locked
uv run pytest
```

The tests cover the generator plan, the DynamoDB write rule, the S3 layout, the consumer loop, the Spark job and every IAM policy. AWS calls go to moto, and Kafka and CloudWatch to in-memory fakes, so no test needs an AWS account or a broker.

Deploying the real thing needs an AWS account, a Pulumi account and a host for the Docker Compose stack. [docs/runbook.md](docs/runbook.md) covers the VPS install, the health checks, how to test each alarm, and the changes that need admin credentials. The three saved Athena queries are [sql/disruptions_last_3_days.sql](sql/disruptions_last_3_days.sql), [sql/delayed_flights_yesterday.sql](sql/delayed_flights_yesterday.sql) and [sql/passenger_journey.sql](sql/passenger_journey.sql).

## Cost

Estimated monthly cost at this volume, before any credits. Measured figures replace this after the first full month.

| Service | Use | USD / month |
|---------|-----|-------------|
| DynamoDB | 5 read and 5 write units, provisioned | 0 (always-free tier) |
| Glue | one Flex run a day, about 5 DPU-minutes (0.07 to 0.08 DPU-hours measured) | ~0.70 |
| S3 | a few MB, plus PUT requests from the consumer and CloudTrail | ~0.10 |
| Athena | KBs scanned per query | ~0 |
| CloudWatch | 10 custom metrics, 2 alarms, 1 dashboard | 0 (free tier) |
| SNS, EventBridge, Budgets, CloudTrail | one email topic, one rule, one budget, one trail | 0 |
| Total | | under 1 |

The VPS is shared with other projects and not counted.

## What it does not do

* An event that arrives after the 02:30 UTC run three days after its flight date never reaches the curated tables. The generator's worst case is under two days, so only a real feed would hit this.
* One Kafka broker on one box, with no replication. If the VPS disk fails, events not yet archived are lost.
* No schema registry. The three schemas live in `src/fda/schemas.py` and every module reads them from there.
* The two stores can still disagree on a malformed event. The consumer rejects an event with a decimal in `delay_minutes` or `amount_cents`, while the daily job keeps it with that field empty. Both drop an event with no `event_id` or a non-whole `sequence`.
* If the generator is down for part of a day, the bookings for a flight whose events were all skipped do not show up in `journeys` for that day.
* CI can create new `fda-` roles and users. They stay inside the permissions boundary, so they cannot reach IAM or STS. A compromised CI run could still leave one behind as a way back in.
* CI deploys with a personal Pulumi access token stored as a GitHub secret.

[docs/decisions.md](docs/decisions.md) records why it is built this way and what the alternatives cost.

## Stack

Python 3.11, PySpark 3.5 on Glue 5.0, Apache Kafka 4.3 (KRaft), confluent-kafka, boto3, DynamoDB, S3, Parquet, Athena, Pulumi, IAM with GitHub OIDC, GitHub Actions, CloudWatch, SNS, EventBridge, CloudTrail, Docker Compose, uv, ruff, pytest, moto.

[MIT licence](LICENSE).
