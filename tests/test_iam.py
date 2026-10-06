from fnmatch import fnmatchcase

import iam

ACCOUNT = "123456789012"
REGION = "eu-central-1"
VPS_IP = "203.0.113.7"
TABLE_ARN = f"arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/fda-live"
BUCKET_ARN = "arn:aws:s3:::fda-bucket"
SUBJECT = "repo:athbol@32675046/flight-disruption-aws@1406409508"
SUB = f"{SUBJECT}:ref:refs/heads/main"
OIDC_ARN = f"arn:aws:iam::{ACCOUNT}:oidc-provider/token.actions.githubusercontent.com"
TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT}:fda-alerts"
RULE_ARN = f"arn:aws:events:{REGION}:{ACCOUNT}:rule/fda-glue-failed"
ALARM_ARNS = [
    f"arn:aws:cloudwatch:{REGION}:{ACCOUNT}:alarm:fda-heartbeat-missing",
    f"arn:aws:cloudwatch:{REGION}:{ACCOUNT}:alarm:fda-dynamodb-throttles",
]


def as_list(value):
    return value if isinstance(value, list) else [value]


def actions(statement):
    return as_list(statement["Action"])


def resources(statement):
    return as_list(statement["Resource"])


def all_resources(policy):
    return {resource for statement in policy["Statement"] for resource in resources(statement)}


def all_actions(policy):
    return {action for statement in policy["Statement"] for action in actions(statement)}


def consumer():
    return iam.consumer_policy(TABLE_ARN, BUCKET_ARN, VPS_IP)


def glue():
    return iam.glue_policy(BUCKET_ARN, REGION, ACCOUNT)


def test_consumer_resources_are_exactly_table_raw_prefix_and_metrics():
    assert all_resources(consumer()) == {TABLE_ARN, f"{BUCKET_ARN}/raw/*", "*"}


def test_consumer_cannot_touch_bucket_root_or_curated():
    found = all_resources(consumer())
    assert BUCKET_ARN not in found
    assert f"{BUCKET_ARN}/*" not in found
    assert not any("curated" in resource for resource in found)


def test_consumer_statements_all_require_the_vps_ip():
    for statement in consumer()["Statement"]:
        assert statement["Condition"]["IpAddress"]["aws:SourceIp"] == f"{VPS_IP}/32"


def test_consumer_wildcard_resource_is_only_metrics_in_our_namespace():
    for statement in consumer()["Statement"]:
        if "*" in resources(statement):
            assert actions(statement) == ["cloudwatch:PutMetricData"]
            namespace = statement["Condition"]["StringEquals"]["cloudwatch:namespace"]
            assert namespace == "FlightDisruption"


def test_consumer_actions_are_exactly_these_per_resource():
    granted = {tuple(resources(s)): set(actions(s)) for s in consumer()["Statement"]}
    assert granted == {
        (TABLE_ARN,): {"dynamodb:PutItem", "dynamodb:Query", "dynamodb:BatchGetItem"},
        (f"{BUCKET_ARN}/raw/*",): {"s3:PutObject"},
        ("*",): {"cloudwatch:PutMetricData"},
    }


def test_glue_cannot_write_raw_or_artifacts():
    for statement in glue()["Statement"]:
        writes = {"s3:PutObject", "s3:DeleteObject"} & set(actions(statement))
        if writes:
            assert f"{BUCKET_ARN}/raw/*" not in resources(statement)
            assert f"{BUCKET_ARN}/artifacts/*" not in resources(statement)


def test_glue_writes_exactly_curated_and_its_folder_marker():
    writes = [
        set(resources(statement))
        for statement in glue()["Statement"]
        if {"s3:PutObject", "s3:DeleteObject"} & set(actions(statement))
    ]
    assert writes == [
        {BUCKET_ARN, f"{BUCKET_ARN}/curated/*", f"{BUCKET_ARN}/curated_$folder$"},
    ]


def test_glue_resources_stay_inside_bucket_logs_and_fda_catalog():
    allowed = (
        BUCKET_ARN,
        f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/aws-glue/",
        f"arn:aws:glue:{REGION}:{ACCOUNT}:catalog",
        f"arn:aws:glue:{REGION}:{ACCOUNT}:database/fda",
        f"arn:aws:glue:{REGION}:{ACCOUNT}:table/fda/",
    )
    for resource in all_resources(glue()):
        assert resource.startswith(allowed), resource


def test_glue_has_no_iam_or_s3_wildcard_actions():
    found = all_actions(glue())
    assert not any(action.startswith("iam:") for action in found)
    assert "s3:*" not in found


def test_github_sub_pins_owner_and_repo_ids():
    assert iam.github_sub("ref:refs/heads/main") == f"{SUBJECT}:ref:refs/heads/main"
    assert iam.github_sub("pull_request") == f"{SUBJECT}:pull_request"


def test_github_subs_have_no_wildcards():
    for ref in ("ref:refs/heads/main", "pull_request"):
        assert "*" not in iam.github_sub(ref)


def test_github_trust_pins_sub_aud_and_provider():
    trust = iam.github_trust(ACCOUNT, SUB)
    [statement] = trust["Statement"]
    assert statement["Action"] == "sts:AssumeRoleWithWebIdentity"
    assert statement["Principal"] == {"Federated": OIDC_ARN}
    assert statement["Condition"] == {
        "StringEquals": {
            "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
            "token.actions.githubusercontent.com:sub": SUB,
        }
    }


def test_deploy_allows_only_fda_named_iam_resources():
    fda_prefixes = tuple(
        f"arn:aws:iam::{ACCOUNT}:{kind}/fda-"
        for kind in ("role", "user", "policy", "instance-profile")
    )
    policy = iam.deploy_policy(ACCOUNT)
    for statement in policy["Statement"]:
        if statement["Effect"] != "Allow":
            continue
        for resource in resources(statement):
            assert resource.startswith(fda_prefixes) or resource == OIDC_ARN, resource


def test_deploy_can_pass_only_fda_roles():
    policy = iam.deploy_policy(ACCOUNT)
    passing = [s for s in policy["Statement"] if "iam:PassRole" in actions(s)]
    assert passing
    for statement in passing:
        assert resources(statement) == [f"arn:aws:iam::{ACCOUNT}:role/fda-*"]


def test_deploy_cannot_change_its_own_roles_or_the_oidc_provider():
    policy = iam.deploy_policy(ACCOUNT)
    [deny] = [s for s in policy["Statement"] if OIDC_ARN in resources(s) and s["Effect"] == "Deny"]
    assert set(actions(deny)) == {
        "iam:Update*",
        "iam:Put*",
        "iam:Attach*",
        "iam:Detach*",
        "iam:Delete*",
        "iam:Create*",
    }
    gha_roles = [
        r for r in resources(deny) if r.startswith(f"arn:aws:iam::{ACCOUNT}:role/fda-gha-")
    ]
    assert gha_roles == [f"arn:aws:iam::{ACCOUNT}:role/fda-gha-*"]
    assert OIDC_ARN in resources(deny)


BOUNDARY_ARN = f"arn:aws:iam::{ACCOUNT}:policy/fda-boundary"


def denied(policy, action, resource):
    return any(
        statement["Effect"] == "Deny"
        and action in actions(statement)
        and resource in resources(statement)
        for statement in policy["Statement"]
    )


GRANTING = (
    "iam:CreateRole",
    "iam:CreateUser",
    "iam:PutRolePolicy",
    "iam:PutUserPolicy",
    "iam:AttachRolePolicy",
    "iam:AttachUserPolicy",
    "iam:PutRolePermissionsBoundary",
    "iam:PutUserPermissionsBoundary",
)


def test_deploy_creates_and_grants_identities_only_with_the_boundary():
    policy = iam.deploy_policy(ACCOUNT)
    condition = {"StringEquals": {"iam:PermissionsBoundary": BOUNDARY_ARN}}
    for statement in policy["Statement"]:
        if statement["Effect"] != "Allow" or statement.get("Condition") == condition:
            continue
        for pattern in actions(statement):
            for action in GRANTING:
                assert not fnmatchcase(action, pattern), (pattern, action)
    assert not any("iam:*" in actions(s) for s in policy["Statement"] if s["Effect"] == "Allow")


def test_deploy_cannot_remove_boundaries_or_change_the_boundary_policy():
    policy = iam.deploy_policy(ACCOUNT)
    assert denied(policy, "iam:DeleteRolePermissionsBoundary", "*")
    assert denied(policy, "iam:DeleteUserPermissionsBoundary", "*")
    for action in ("iam:CreatePolicyVersion", "iam:DeletePolicy", "iam:SetDefaultPolicyVersion"):
        matching = [
            s for s in policy["Statement"] if s["Effect"] == "Deny" and BOUNDARY_ARN in resources(s)
        ]
        assert any(
            action.startswith(pattern.rstrip("*")) for s in matching for pattern in actions(s)
        ), action


def test_deploy_cannot_stop_the_trail_or_change_the_budget():
    policy = iam.deploy_policy(ACCOUNT)
    for action in (
        "cloudtrail:StopLogging",
        "cloudtrail:DeleteTrail",
        "cloudtrail:UpdateTrail",
        "cloudtrail:PutEventSelectors",
        "cloudtrail:PutInsightSelectors",
        "budgets:ModifyBudget",
        "budgets:DeleteBudget",
    ):
        assert denied(policy, action, "*"), action


def test_boundary_allows_only_the_data_services_and_no_identity_actions():
    boundary = iam.boundary_policy()
    assert all(statement["Effect"] == "Allow" for statement in boundary["Statement"])
    assert all_resources(boundary) == {"*"}
    assert all_actions(boundary) == {
        "s3:*",
        "dynamodb:*",
        "glue:*",
        "athena:*",
        "logs:*",
        "cloudwatch:*",
        "events:*",
        "sns:*",
    }
    assert not any(action.startswith(("iam:", "sts:")) for action in all_actions(boundary))


def alerts():
    return iam.alerts_publish(TOPIC_ARN, RULE_ARN, ALARM_ARNS)


def test_alerts_topic_only_takes_publish_from_events_and_cloudwatch():
    principals = [statement["Principal"] for statement in alerts()["Statement"]]
    assert principals == [
        {"Service": "events.amazonaws.com"},
        {"Service": "cloudwatch.amazonaws.com"},
    ]
    assert all_actions(alerts()) == {"sns:Publish"}
    assert all_resources(alerts()) == {TOPIC_ARN}


def test_alerts_events_publish_only_from_the_glue_rule():
    events = alerts()["Statement"][0]
    assert events["Condition"] == {"ArnEquals": {"aws:SourceArn": RULE_ARN}}


def test_alerts_cloudwatch_publishes_only_from_our_alarms():
    cloudwatch = alerts()["Statement"][1]
    assert cloudwatch["Condition"] == {"ArnEquals": {"aws:SourceArn": ALARM_ARNS}}


def test_alerts_statements_have_unique_sids():
    sids = [statement.get("Sid") for statement in alerts()["Statement"]]
    assert sids == ["EventsPublish", "AlarmsPublish"]


def test_glue_trust_only_for_glue_in_our_account():
    [statement] = iam.glue_trust(ACCOUNT)["Statement"]
    assert statement["Principal"] == {"Service": "glue.amazonaws.com"}
    assert statement["Action"] == "sts:AssumeRole"
    assert statement["Condition"] == {"StringEquals": {"aws:SourceAccount": ACCOUNT}}


def test_preview_cannot_read_data_logs_or_secrets():
    preview = iam.preview_deny()
    assert {statement["Effect"] for statement in preview["Statement"]} == {"Deny"}
    assert all_resources(preview) == {"*"}
    assert all_actions(preview) == {
        "s3:GetObject",
        "dynamodb:GetItem",
        "dynamodb:Query",
        "dynamodb:Scan",
        "dynamodb:BatchGetItem",
        "athena:GetQueryResults",
        "logs:GetLogEvents",
        "logs:FilterLogEvents",
        "ssm:GetParameter*",
        "secretsmanager:GetSecretValue",
        "s3:GetObjectVersion",
        "dynamodb:PartiQLSelect",
        "logs:StartQuery",
        "logs:GetQueryResults",
        "logs:StartLiveTail",
        "logs:GetLogRecord",
    }


def test_bucket_refuses_requests_without_tls():
    [statement] = iam.tls_only(BUCKET_ARN)["Statement"]
    assert statement["Effect"] == "Deny"
    assert statement["Principal"] == "*"
    assert actions(statement) == ["s3:*"]
    assert set(resources(statement)) == {BUCKET_ARN, f"{BUCKET_ARN}/*"}
    assert statement["Condition"] == {"Bool": {"aws:SecureTransport": "false"}}


TRAIL_BUCKET_ARN = "arn:aws:s3:::fda-trail"
TRAIL_ARN = f"arn:aws:cloudtrail:{REGION}:{ACCOUNT}:trail/fda"


def test_trail_bucket_takes_only_our_trail_writes():
    trail = iam.trail_bucket_policy(TRAIL_BUCKET_ARN, TRAIL_ARN, ACCOUNT)
    acl_check, write = trail["Statement"]
    for statement in (acl_check, write):
        assert statement["Effect"] == "Allow"
        assert statement["Principal"] == {"Service": "cloudtrail.amazonaws.com"}
        assert statement["Condition"]["StringEquals"]["aws:SourceArn"] == TRAIL_ARN
    assert actions(acl_check) == ["s3:GetBucketAcl"]
    assert resources(acl_check) == [TRAIL_BUCKET_ARN]
    assert actions(write) == ["s3:PutObject"]
    assert resources(write) == [f"{TRAIL_BUCKET_ARN}/AWSLogs/{ACCOUNT}/*"]
    assert write["Condition"]["StringEquals"]["s3:x-amz-acl"] == "bucket-owner-full-control"
