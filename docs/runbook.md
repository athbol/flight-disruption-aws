# Runbook

## VPS

The generator, Kafka and the consumer run as a Docker Compose stack on the VPS (SSH alias `arkitsa`, user `deploy`).
The `deploy` user is not in the docker group, so every Docker command uses `sudo`.
Nothing listens on the host. The containers talk over the compose network.

### First install

1. Clone the repo as root.

   ```sh
   sudo git clone https://github.com/athbol/flight-disruption-aws /opt/flight-disruption-aws
   ```

   Verify: `ls /opt/flight-disruption-aws/compose.yaml`.

2. Write `/opt/flight-disruption-aws/.env` with `RAW_BUCKET`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
   The values come from the Pulumi stack outputs `bucket`, `consumer_access_key_id` and `consumer_secret_access_key`.
   Run this on the laptop from the repo root. It pipes the values straight into a root-owned file with mode 600, so the secret never reaches the screen.

   ```sh
   pulumi stack output --stack athbol-projects/flight-disruption-aws/prod --show-secrets --json \
     | python3 -c 'import json, sys; o = json.load(sys.stdin); [print(k + "=" + o[v]) for k, v in [("RAW_BUCKET", "bucket"), ("AWS_ACCESS_KEY_ID", "consumer_access_key_id"), ("AWS_SECRET_ACCESS_KEY", "consumer_secret_access_key")]]' \
     | ssh arkitsa 'sudo install -m 600 -o root -g root /dev/stdin /opt/flight-disruption-aws/.env'
   ```

   Verify: `sudo wc -l /opt/flight-disruption-aws/.env` shows 3 lines and `sudo stat -c "%a %U" /opt/flight-disruption-aws/.env` shows `600 root`.
   Do not print the file.

3. Install and start the systemd unit.

   ```sh
   cd /opt/flight-disruption-aws
   sudo cp deploy/fda.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now fda
   ```

   Verify: `systemctl is-active fda` shows `active`.

### Checks

1. All three containers are up and Kafka is healthy.

   ```sh
   cd /opt/flight-disruption-aws && sudo docker compose ps
   ```

   Verify: `kafka` shows `(healthy)`, `generator` and `consumer` show `Up`.

2. The consumer is running without errors.

   ```sh
   sudo docker compose logs --tail 20 consumer
   ```

   Verify: no tracebacks.

3. Raw events land in S3. Run this on the laptop from the repo root with admin credentials.

   ```sh
   bucket=$(pulumi stack output --stack athbol-projects/flight-disruption-aws/prod bucket)
   aws s3 ls "s3://$bucket/raw/topic=flights/" --recursive | tail
   ```

   Verify: objects with recent timestamps.
   The generator emits events at their scheduled times, so a gap of an hour between objects during the day is normal.

4. Live state lands in DynamoDB.

   ```sh
   aws dynamodb scan --table-name fda-live --max-items 3 \
     --query 'Items[*].[pk.S,sk.S,event_type.S,sequence.N]' --output table
   ```

   Verify: rows with flight, booking or ticket keys.

5. The consumer sends its heartbeat metric.

   ```sh
   aws cloudwatch get-metric-statistics --namespace FlightDisruption --metric-name Heartbeat \
     --start-time "$(date -u -v-10M +%Y-%m-%dT%H:%M:%SZ)" --end-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
     --period 60 --statistics Sum
   ```

   Verify: datapoints for the last minutes. On Linux use `date -u -d '-10 min'` instead of `date -u -v-10M`.

### Update

```sh
cd /opt/flight-disruption-aws && sudo git pull && sudo docker compose up -d --build
```

This rebuilds the image and recreates only the containers whose image or config changed, usually the generator and the consumer.
The systemd unit does not build at boot, so this step is the only place a new image is built.
If `deploy/fda.service` changed, copy it again and reload systemd.

```sh
sudo cp deploy/fda.service /etc/systemd/system/ && sudo systemctl daemon-reload
```

Verify: `sudo docker compose ps` shows all three containers up and Kafka healthy.

## Glue and Athena

The `fda-curate` job runs daily at 02:30 UTC and rewrites the last four days of `curated/`.

Run it by hand for today, or pass a date:

```sh
aws glue start-job-run --job-name fda-curate
aws glue start-job-run --job-name fda-curate --arguments '{"--RUN_DATE":"2026-10-06"}'
```

Verify: `aws glue get-job-runs --job-name fda-curate --max-items 1 --query 'JobRuns[0].JobRunState'` shows `SUCCEEDED`.

Query by hand in the `fda` workgroup (saved queries are in `sql/`):

```sh
aws athena start-query-execution --work-group fda \
  --query-execution-context Database=fda \
  --query-string "$(cat sql/disruptions_last_3_days.sql)"
aws athena get-query-results --query-execution-id <id>
```

Verify: `aws athena get-query-execution --query-execution-id <id>` shows `SUCCEEDED` and the results have one row per disruption type.

## Monitoring

Dashboard: https://eu-central-1.console.aws.amazon.com/cloudwatch/home?region=eu-central-1#dashboards:name=fda

Every alert goes to the `fda-alerts` email subscription.

* `fda-heartbeat-missing` fires when the consumer sends no heartbeat for two 5-minute periods in a row. It emails again when the heartbeat comes back.
* `fda-dynamodb-throttles` fires when any `PutItem` on `fda-live` is throttled in a 5-minute period.
* The EventBridge rule `fda-glue-failed` emails when `fda-curate` ends in `FAILED`, `TIMEOUT` or `ERROR`.

### Test the heartbeat alarm

On the VPS, stop the consumer, wait for the email, then start it again.

```sh
cd /opt/flight-disruption-aws && sudo docker compose stop consumer
sudo docker compose start consumer
```

Verify: an ALARM email arrives within about 10 minutes of the stop, and an OK email follows within about 10 minutes of the start.
`aws cloudwatch describe-alarms --alarm-names fda-heartbeat-missing --query 'MetricAlarms[0].StateValue'` shows `ALARM`, then `OK`.

### Test the Glue failure rule

Point the job at a bucket it cannot read.

```sh
aws glue start-job-run --job-name fda-curate --arguments '{"--RAW_ROOT":"s3://nope/raw"}'
```

Verify: `aws glue get-job-runs --job-name fda-curate --max-items 1 --query 'JobRuns[0].JobRunState'` shows `FAILED` and a Glue Job State Change email arrives within minutes.

### Throttle alarm

There is no safe way to force throttling on the live table, so this alarm is not tested by hand.

Verify: `aws cloudwatch describe-alarms --alarm-names fda-dynamodb-throttles --query 'MetricAlarms[0].StateValue'` shows `OK`.
