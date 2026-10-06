import json
from pathlib import Path

import catalog
import dashboard
import iam
import pulumi
import pulumi_aws as aws

from fda.consumer import HEARTBEAT, METRIC_NAMESPACE
from fda.live import TABLE_NAME
from fda.raw import RAW_PREFIX
from fda.schemas import CURATED, PARTITION_KEY

GITHUB_THUMBPRINT = "6938fd4d98bab03faadb97b34396831e3780aea1"
FDA_MODULES = ("__init__.py", "schemas.py", "curate.py")
HIVE_PARQUET = "org.apache.hadoop.hive.ql.io.parquet"
NAMED_QUERIES = ("disruptions_last_3_days", "passenger_journey", "delayed_flights_yesterday")

config = pulumi.Config("fda")
vps_ip = config.require_secret("vps_ip")
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
            "filter": {"prefix": f"{RAW_PREFIX}/"},
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
aws.s3.BucketPolicy(
    "bucket-tls-only",
    bucket=bucket.id,
    policy=bucket.arn.apply(lambda arn: json.dumps(iam.tls_only(arn))),
)

table = aws.dynamodb.Table(
    "live",
    name=TABLE_NAME,
    hash_key="pk",
    range_key="sk",
    attributes=[{"name": "pk", "type": "S"}, {"name": "sk", "type": "S"}],
    billing_mode="PROVISIONED",
    read_capacity=5,
    write_capacity=5,
    point_in_time_recovery={"enabled": False},
    deletion_protection_enabled=False,
)

boundary = aws.iam.Policy("boundary", name="fda-boundary", policy=json.dumps(iam.boundary_policy()))

consumer = aws.iam.User("consumer", name="fda-consumer", permissions_boundary=boundary.arn)
aws.iam.UserPolicy(
    "consumer-policy",
    user=consumer.name,
    policy=pulumi.Output.all(table.arn, bucket.arn, vps_ip).apply(
        lambda args: json.dumps(iam.consumer_policy(*args))
    ),
)
consumer_key = aws.iam.AccessKey("consumer-key-2", user=consumer.name)

glue_role = aws.iam.Role(
    "glue",
    name="fda-glue",
    permissions_boundary=boundary.arn,
    assume_role_policy=json.dumps(iam.glue_trust(account)),
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
    for name in ("output", "error")
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
        "--RAW_ROOT": pulumi.Output.format("s3://{0}/{1}", bucket.bucket, RAW_PREFIX),
        "--CURATED_ROOT": pulumi.Output.format("s3://{0}/curated", bucket.bucket),
        "--RUN_DATE": "today",
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
for name in CURATED:
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
            f"projection.{PARTITION_KEY}.type": "date",
            f"projection.{PARTITION_KEY}.format": "yyyy-MM-dd",
            f"projection.{PARTITION_KEY}.range": "2026-10-01,NOW",
            f"projection.{PARTITION_KEY}.interval": "1",
            f"projection.{PARTITION_KEY}.interval.unit": "DAYS",
            "storage.location.template": pulumi.Output.format(
                "{0}{1}=${{{1}}}", location, PARTITION_KEY
            ),
        },
    )

workgroup = aws.athena.Workgroup(
    "athena",
    name="fda",
    force_destroy=True,
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

heartbeat_alarm = aws.cloudwatch.MetricAlarm(
    "heartbeat-missing",
    name="fda-heartbeat-missing",
    alarm_description="The consumer stopped publishing. Check docker compose ps on the VPS.",
    namespace=METRIC_NAMESPACE,
    metric_name=HEARTBEAT,
    statistic="Sum",
    period=300,
    evaluation_periods=2,
    threshold=1,
    comparison_operator="LessThanThreshold",
    treat_missing_data="breaching",
    alarm_actions=[alerts.arn],
    ok_actions=[alerts.arn],
)
throttle_alarm = aws.cloudwatch.MetricAlarm(
    "dynamodb-throttles",
    name="fda-dynamodb-throttles",
    namespace="AWS/DynamoDB",
    metric_name="WriteThrottleEvents",
    dimensions={"TableName": table.name},
    statistic="Sum",
    period=300,
    evaluation_periods=1,
    threshold=0,
    comparison_operator="GreaterThanThreshold",
    treat_missing_data="notBreaching",
    alarm_actions=[alerts.arn],
)
glue_failed = aws.cloudwatch.EventRule(
    "glue-failed",
    name="fda-glue-failed",
    event_pattern=json.dumps(
        {
            "source": ["aws.glue"],
            "detail-type": ["Glue Job State Change"],
            "detail": {"jobName": ["fda-curate"], "state": ["FAILED", "TIMEOUT", "ERROR"]},
        }
    ),
)
aws.cloudwatch.EventTarget("glue-failed-email", rule=glue_failed.name, arn=alerts.arn)
aws.sns.TopicPolicy(
    "alerts-publish",
    arn=alerts.arn,
    policy=pulumi.Output.all(
        alerts.arn, glue_failed.arn, heartbeat_alarm.arn, throttle_alarm.arn
    ).apply(lambda arns: json.dumps(iam.alerts_publish(arns[0], arns[1], arns[2:]))),
)
aws.cloudwatch.Dashboard(
    "dashboard",
    dashboard_name="fda",
    dashboard_body=table.name.apply(lambda name: json.dumps(dashboard.dashboard(region, name))),
)

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
    url=f"https://{iam.GITHUB_OIDC_HOST}",
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
aws.iam.RolePolicy(
    "gha-preview-no-data",
    role=preview_role.id,
    policy=json.dumps(iam.preview_deny()),
)

trail_bucket = aws.s3.Bucket("trail-bucket", bucket=f"fda-{account}-trail")
aws.s3.BucketPublicAccessBlock(
    "trail-bucket-public-access",
    bucket=trail_bucket.id,
    block_public_acls=True,
    block_public_policy=True,
    ignore_public_acls=True,
    restrict_public_buckets=True,
)
aws.s3.BucketServerSideEncryptionConfiguration(
    "trail-bucket-encryption",
    bucket=trail_bucket.id,
    rules=[{"apply_server_side_encryption_by_default": {"sse_algorithm": "AES256"}}],
)
aws.s3.BucketLifecycleConfiguration(
    "trail-bucket-lifecycle",
    bucket=trail_bucket.id,
    rules=[
        {
            "id": "expire-trail",
            "status": "Enabled",
            "filter": {"prefix": ""},
            "expiration": {"days": 90},
        }
    ],
)
trail_arn = f"arn:aws:cloudtrail:{region}:{account}:trail/fda"
trail_policy = aws.s3.BucketPolicy(
    "trail-bucket-policy",
    bucket=trail_bucket.id,
    policy=trail_bucket.arn.apply(
        lambda arn: json.dumps(iam.trail_bucket_policy(arn, trail_arn, account))
    ),
)
aws.cloudtrail.Trail(
    "trail",
    name="fda",
    s3_bucket_name=trail_bucket.id,
    is_multi_region_trail=True,
    include_global_service_events=True,
    enable_log_file_validation=True,
    opts=pulumi.ResourceOptions(depends_on=[trail_policy]),
)
aws.accessanalyzer.Analyzer("access-analyzer", analyzer_name="fda", type="ACCOUNT")

pulumi.export("bucket", bucket.bucket)
pulumi.export("table", table.name)
pulumi.export("alerts_topic_arn", alerts.arn)
pulumi.export("deploy_role_arn", deploy_role.arn)
pulumi.export("preview_role_arn", preview_role.arn)
pulumi.export("consumer_access_key_id", pulumi.Output.secret(consumer_key.id))
pulumi.export("consumer_secret_access_key", pulumi.Output.secret(consumer_key.secret))
