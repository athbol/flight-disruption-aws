# Decisions

Each entry says what was chosen, what it was chosen over, and what it costs.

## Kafka on the VPS, not MSK

MSK Serverless starts at about 0.75 USD per cluster-hour, which is over 500 USD a month for a feed of a few thousand events a day. A single `apache/kafka` container in KRaft mode on a VPS that already exists costs nothing extra and uses about 380 MiB of RAM. The repo keeps the Kafka client code and the consumer semantics that would move to MSK unchanged.

Cost: one broker, no replication, and the broker lives outside AWS. The consumer's IAM user is restricted to the VPS address to make up for the key living off-cloud.

## Glue, not EMR or a Lambda

The curate step is one Spark job that runs once a day for about two minutes. Glue charges by the DPU-second with no cluster to keep up, and the Flex execution class halves the rate for a job that can wait a few minutes to start. EMR Serverless would work too, but it needs an application, a VPC decision and more IAM for the same job. A Lambda with pandas would be cheaper still, but the job is written in PySpark on purpose: the join and window logic is the part worth showing.

Cost: a cold start of one to two minutes on every run, and Glue 5.0 pins the code to Python 3.11.

## Commit offsets after the S3 flush

The consumer could commit after every DynamoDB write and treat the archive as best effort. Instead it buffers records, uploads them, and only then commits. If it dies after the upload and before the commit, the next start replays the buffer. Replays are safe because every event has a stable id and a sequence, so DynamoDB rejects the stale copies and the daily job drops the duplicate rows.

Cost: at-least-once, so the archive can hold duplicate lines, and the live table does a few extra conditional writes after a crash.

## Partition projection, not a crawler

The curated tables have one partition key, `flight_date`, with one value per day. Partition projection tells Athena the pattern and the date range, so no crawler runs and no partition is registered. The job's dynamic partition overwrite writes straight to the path Athena expects.

Cost: the projection range starts at a fixed date and ends at `NOW`, and a wrong `storage.location.template` fails silently with empty results, so one test pins the template.

## Provisioned 5/5, not on-demand

The live table is in the DynamoDB always-free tier at 5 read and 5 write capacity units. The consumer writes a few thousand items a day, well under one write per second on average, with short bursts at midnight when the day's bookings arrive. On-demand would also cost close to nothing here but it is not free, and a throttle alarm makes the provisioned limit visible if the volume ever grows.

Cost: a burst above 5 writes a second is throttled and retried by boto3. The alarm emails if that happens.

## Flex execution for Glue

The job runs at 02:30 UTC and nobody waits for it. Flex can delay the start and can lose executors, which Glue retries. The job is idempotent, so a retry is safe.

Cost: no guarantee on when the run finishes, and a failed Flex run is a real failure that needs a manual re-run. The EventBridge rule emails on FAILED, TIMEOUT or ERROR.

## A permissions boundary on everything CI creates

The deploy role has PowerUserAccess plus IAM rights on names starting with `fda-`. Without a boundary that is full admin with extra steps: CI could create a role, attach `AdministratorAccess`, and assume it. The managed policy `fda-boundary` is required on every identity the deploy role creates and allows only the services this project uses, with nothing from IAM or STS. The deploy role is denied from removing a boundary, changing the boundary policy, changing itself, or touching the CloudTrail trail and the budget.

Cost: changes to the deploy role, the boundary, the OIDC provider, the trail and the budget have to be applied from a laptop with admin credentials, which the runbook describes.

## Deterministic generator instead of recorded data

The generator plans a whole day from `hash(seed, date)`. The same seed and date produce the same flights, passengers, outcomes, lags and duplicates on every machine. Tests assert exact counts, the daily job can be re-run against a known day, and nothing in the repo came from a real system.

Cost: the data has no seasonality and only six routes, and a generator restart in the middle of a day skips the emissions that were due while it was down.
