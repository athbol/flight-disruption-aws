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

3. Raw events land in S3. Run this on the laptop with admin credentials.

   ```sh
   aws s3 ls s3://fda-597595167166-euc1/raw/topic=flights/ --recursive | tail
   ```

   Verify: objects with recent timestamps.

4. The consumer sends its heartbeat metric.

   ```sh
   aws cloudwatch get-metric-statistics --namespace FlightDisruption --metric-name Heartbeat \
     --start-time "$(date -u -v-10M +%Y-%m-%dT%H:%M:%SZ)" --end-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
     --period 60 --statistics Sum
   ```

   Verify: datapoints for the last minutes. On Linux use `date -u -d '-10 min'` instead of `date -u -v-10M`.

### Update

```sh
cd /opt/flight-disruption-aws && sudo git pull && sudo systemctl restart fda
```

Verify: `sudo docker compose ps` shows all three containers recently started and Kafka healthy.
