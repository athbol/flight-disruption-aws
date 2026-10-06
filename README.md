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
<tr>
<td width="50%" valign="top">10. The 1 USD monthly budget.<br><img src="docs/screenshots/10-budget.png" alt="Budget"></td>
<td width="50%" valign="top"></td>
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
