import json

import iam
import pulumi
import pulumi_aws as aws

GITHUB_OIDC_URL = "https://token.actions.githubusercontent.com"
GITHUB_THUMBPRINT = "6938fd4d98bab03faadb97b34396831e3780aea1"

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
