import json
from pathlib import Path

import catalog
import iam
import pulumi
import pulumi_aws as aws

GITHUB_OIDC_URL = "https://token.actions.githubusercontent.com"
GITHUB_THUMBPRINT = "6938fd4d98bab03faadb97b34396831e3780aea1"
FDA_MODULES = ("__init__.py", "schemas.py", "curate.py")
HIVE_PARQUET = "org.apache.hadoop.hive.ql.io.parquet"
NAMED_QUERIES = ("disruptions_last_3_days", "passenger_journey", "delayed_flights_today")

config = pulumi.Config("fda")
vps_ip = config.require("vps_ip")
alert_email = config.require_secret("alert_email")
account = aws.get_caller_identity().account_id
region = aws.get_region().region

bucket = aws.s3.Bucket("bucket", bucket=f"fda-{account}-euc1")
aws.s3.BucketPublicAccessBlock(
    "bucket-public-access",
    bucket=bucket.id,
    block_public_acls=True,
    block_public_policy=True,
    ignore_public_acls=True,
    restrict_public_buckets=True,
)
aws.s3.BucketServerSideEncryptionConfiguration(
    "bucket-encryption",
    bucket=bucket.id,
    rules=[{"apply_server_side_encryption_by_default": {"sse_algorithm": "AES256"}}],
)
aws.s3.BucketLifecycleConfiguration(
    "bucket-lifecycle",
    bucket=bucket.id,
    rules=[
        {
            "id": "expire-raw",
            "status": "Enabled",
            "filter": {"prefix": "raw/"},
            "expiration": {"days": 30},
        },
        {
            "id": "expire-athena-results",
            "status": "Enabled",
            "filter": {"prefix": "athena-results/"},
            "expiration": {"days": 7},
        },
        {
            "id": "abort-incomplete-uploads",
            "status": "Enabled",
            "filter": {"prefix": ""},
            "abort_incomplete_multipart_upload": {"days_after_initiation": 7},
        },
    ],
)

table = aws.dynamodb.Table(
    "live",
    name="fda-live",
    hash_key="pk",
    range_key="sk",
    attributes=[{"name": "pk", "type": "S"}, {"name": "sk", "type": "S"}],
    billing_mode="PROVISIONED",
    read_capacity=5,
    write_capacity=5,
    point_in_time_recovery={"enabled": False},
    deletion_protection_enabled=False,
)

consumer = aws.iam.User("consumer", name="fda-consumer")
aws.iam.UserPolicy(
    "consumer-policy",
    user=consumer.name,
    policy=pulumi.Output.all(table.arn, bucket.arn).apply(
        lambda arns: json.dumps(iam.consumer_policy(arns[0], arns[1], vps_ip))
    ),
)
consumer_key = aws.iam.AccessKey("consumer-key", user=consumer.name)

glue_role = aws.iam.Role(
    "glue",
    name="fda-glue",
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "glue.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    ),
)
aws.iam.RolePolicy(
    "glue-policy",
    role=glue_role.id,
    policy=bucket.arn.apply(lambda arn: json.dumps(iam.glue_policy(arn, region, account))),
)

glue_script = aws.s3.BucketObject(
    "glue-script",
    bucket=bucket.id,
    key="artifacts/glue/glue_curate.py",
    source=pulumi.FileAsset("../jobs/glue_curate.py"),
)
glue_package = aws.s3.BucketObject(
    "glue-package",
    bucket=bucket.id,
    key="artifacts/glue/fda.zip",
    source=pulumi.AssetArchive(
        {f"fda/{module}": pulumi.FileAsset(f"../src/fda/{module}") for module in FDA_MODULES}
    ),
)
glue_logs = [
    aws.cloudwatch.LogGroup(f"glue-{name}", name=f"/aws-glue/jobs/{name}", retention_in_days=7)
    for name in ("output", "error", "logs-v2")
]
curate_job = aws.glue.Job(
    "curate",
    name="fda-curate",
    role_arn=glue_role.arn,
    glue_version="5.0",
    worker_type="G.1X",
    number_of_workers=2,
    execution_class="FLEX",
    timeout=15,
    max_retries=0,
    command={
        "name": "glueetl",
        "script_location": pulumi.Output.format("s3://{0}/{1}", bucket.bucket, glue_script.key),
        "python_version": "3",
    },
    default_arguments={
        "--extra-py-files": pulumi.Output.format("s3://{0}/{1}", bucket.bucket, glue_package.key),
        "--RAW_ROOT": pulumi.Output.format("s3://{0}/raw", bucket.bucket),
        "--CURATED_ROOT": pulumi.Output.format("s3://{0}/curated", bucket.bucket),
        "--RUN_DATE": "today",
        "--enable-continuous-cloudwatch-log": "true",
        "--job-language": "python",
    },
    opts=pulumi.ResourceOptions(depends_on=glue_logs),
)
aws.glue.Trigger(
    "curate-daily",
    name="fda-curate-daily",
    type="SCHEDULED",
    schedule="cron(30 2 * * ? *)",
    start_on_creation=True,
    actions=[{"job_name": curate_job.name}],
)

database = aws.glue.CatalogDatabase("catalog", name="fda")
for name in ("journeys", "flights"):
    location = pulumi.Output.format("s3://{0}/curated/{1}/", bucket.bucket, name)
    aws.glue.CatalogTable(
        name,
        name=name,
        database_name=database.name,
        table_type="EXTERNAL_TABLE",
        storage_descriptor={
            "location": location,
            "input_format": f"{HIVE_PARQUET}.MapredParquetInputFormat",
            "output_format": f"{HIVE_PARQUET}.MapredParquetOutputFormat",
            "ser_de_info": {"serialization_library": f"{HIVE_PARQUET}.serde.ParquetHiveSerDe"},
            "columns": catalog.columns(name),
        },
        partition_keys=catalog.partition_keys(),
        parameters={
            "classification": "parquet",
            "projection.enabled": "true",
            "projection.flight_date.type": "date",
            "projection.flight_date.format": "yyyy-MM-dd",
            "projection.flight_date.range": "2026-10-01,NOW",
            "projection.flight_date.interval": "1",
            "projection.flight_date.interval.unit": "DAYS",
            "storage.location.template": pulumi.Output.format(
                "{0}flight_date=${{flight_date}}", location
            ),
        },
    )

workgroup = aws.athena.Workgroup(
    "athena",
    name="fda",
    configuration={
        "enforce_workgroup_configuration": True,
        "publish_cloudwatch_metrics_enabled": True,
        "bytes_scanned_cutoff_per_query": 100_000_000,
        "result_configuration": {
            "output_location": pulumi.Output.format("s3://{0}/athena-results/", bucket.bucket)
        },
    },
)
for query in NAMED_QUERIES:
    aws.athena.NamedQuery(
        query,
        name=query,
        database=database.name,
        workgroup=workgroup.name,
        query=Path(f"../sql/{query}.sql").read_text(),
    )

alerts = aws.sns.Topic("alerts", name="fda-alerts")
aws.sns.TopicSubscription("alerts-email", topic=alerts.arn, protocol="email", endpoint=alert_email)

aws.budgets.Budget(
    "monthly",
    name="fda-monthly",
    budget_type="COST",
    time_unit="MONTHLY",
    limit_amount="1",
    limit_unit="USD",
    cost_types={
        "include_tax": False,
        "include_credit": False,
        "include_refund": False,
        "use_amortized": False,
    },
    notifications=[
        {
            "comparison_operator": "GREATER_THAN",
            "notification_type": "ACTUAL",
            "threshold": 80,
            "threshold_type": "PERCENTAGE",
            "subscriber_email_addresses": [alert_email],
        },
        {
            "comparison_operator": "GREATER_THAN",
            "notification_type": "FORECASTED",
            "threshold": 100,
            "threshold_type": "PERCENTAGE",
            "subscriber_email_addresses": [alert_email],
        },
    ],
)

aws.iam.OpenIdConnectProvider(
    "github",
    url=GITHUB_OIDC_URL,
    client_id_lists=["sts.amazonaws.com"],
    thumbprint_lists=[GITHUB_THUMBPRINT],
)

deploy_role = aws.iam.Role(
    "gha-deploy",
    name="fda-gha-deploy",
    assume_role_policy=json.dumps(iam.github_trust(account, iam.github_sub("ref:refs/heads/main"))),
    max_session_duration=3600,
)
aws.iam.RolePolicyAttachment(
    "gha-deploy-power-user",
    role=deploy_role.name,
    policy_arn="arn:aws:iam::aws:policy/PowerUserAccess",
)
aws.iam.RolePolicy(
    "gha-deploy-iam",
    role=deploy_role.id,
    policy=json.dumps(iam.deploy_policy(account)),
)

preview_role = aws.iam.Role(
    "gha-preview",
    name="fda-gha-preview",
    assume_role_policy=json.dumps(iam.github_trust(account, iam.github_sub("pull_request"))),
)
aws.iam.RolePolicyAttachment(
    "gha-preview-read-only",
    role=preview_role.name,
    policy_arn="arn:aws:iam::aws:policy/ReadOnlyAccess",
)

pulumi.export("bucket", bucket.bucket)
pulumi.export("table", table.name)
pulumi.export("alerts_topic_arn", alerts.arn)
pulumi.export("deploy_role_arn", deploy_role.arn)
pulumi.export("preview_role_arn", preview_role.arn)
pulumi.export("consumer_access_key_id", pulumi.Output.secret(consumer_key.id))
pulumi.export("consumer_secret_access_key", pulumi.Output.secret(consumer_key.secret))
