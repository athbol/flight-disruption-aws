# Runbook

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

Verify: `get-query-execution --query-execution-id <id>` shows `SUCCEEDED` and the results have one row per disruption type.
