GITHUB_OIDC_HOST = "token.actions.githubusercontent.com"
GITHUB_OWNER = "athbol"
GITHUB_OWNER_ID = 32675046
GITHUB_REPO = "flight-disruption-aws"
GITHUB_REPO_ID = 1406409508
METRIC_NAMESPACE = "FlightDisruption"


def policy(statements):
    return {"Version": "2012-10-17", "Statement": statements}


def allow(actions, resources, condition=None):
    statement = {"Effect": "Allow", "Action": actions, "Resource": resources}
    if condition:
        statement["Condition"] = condition
    return statement


def deny(actions, resources):
    return {"Effect": "Deny", "Action": actions, "Resource": resources}


def from_ip(vps_ip, extra=None):
    return {"IpAddress": {"aws:SourceIp": f"{vps_ip}/32"}} | (extra or {})


def oidc_provider_arn(account):
    return f"arn:aws:iam::{account}:oidc-provider/{GITHUB_OIDC_HOST}"


def consumer_policy(table_arn, bucket_arn, vps_ip):
    namespace = {"StringEquals": {"cloudwatch:namespace": METRIC_NAMESPACE}}
    return policy(
        [
            allow(
                ["dynamodb:PutItem", "dynamodb:Query", "dynamodb:BatchGetItem"],
                [table_arn],
                from_ip(vps_ip),
            ),
            allow(["s3:PutObject"], [f"{bucket_arn}/raw/*"], from_ip(vps_ip)),
            allow(["cloudwatch:PutMetricData"], ["*"], from_ip(vps_ip, namespace)),
        ]
    )


def glue_policy(bucket_arn, region, account):
    glue = f"arn:aws:glue:{region}:{account}"
    return policy(
        [
            allow(
                ["s3:GetObject", "s3:ListBucket"],
                [bucket_arn, f"{bucket_arn}/raw/*", f"{bucket_arn}/artifacts/*"],
            ),
            allow(
                ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
                [bucket_arn, f"{bucket_arn}/curated/*", f"{bucket_arn}/curated_$folder$"],
            ),
            allow(
                ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                [f"arn:aws:logs:{region}:{account}:log-group:/aws-glue/*"],
            ),
            allow(
                [
                    "glue:Get*",
                    "glue:CreatePartition",
                    "glue:BatchCreatePartition",
                    "glue:UpdatePartition",
                    "glue:UpdateTable",
                ],
                [f"{glue}:catalog", f"{glue}:database/fda", f"{glue}:table/fda/*"],
            ),
        ]
    )


def github_sub(ref):
    return f"repo:{GITHUB_OWNER}@{GITHUB_OWNER_ID}/{GITHUB_REPO}@{GITHUB_REPO_ID}:{ref}"


def github_trust(account, sub):
    return policy(
        [
            {
                "Effect": "Allow",
                "Principal": {"Federated": oidc_provider_arn(account)},
                "Action": "sts:AssumeRoleWithWebIdentity",
                "Condition": {
                    "StringEquals": {
                        f"{GITHUB_OIDC_HOST}:aud": "sts.amazonaws.com",
                        f"{GITHUB_OIDC_HOST}:sub": sub,
                    },
                },
            }
        ]
    )


def deploy_policy(account):
    iam = f"arn:aws:iam::{account}"
    return policy(
        [
            allow(
                ["iam:*"],
                [
                    f"{iam}:role/fda-*",
                    f"{iam}:user/fda-*",
                    f"{iam}:policy/fda-*",
                    f"{iam}:instance-profile/fda-*",
                ],
            ),
            allow(
                ["iam:GetOpenIDConnectProvider", "iam:TagOpenIDConnectProvider"],
                [oidc_provider_arn(account)],
            ),
            allow(["iam:PassRole"], [f"{iam}:role/fda-*"]),
            deny(
                [
                    "iam:Update*",
                    "iam:Put*",
                    "iam:Attach*",
                    "iam:Detach*",
                    "iam:Delete*",
                    "iam:Create*",
                ],
                [f"{iam}:role/fda-gha-*", oidc_provider_arn(account)],
            ),
        ]
    )
