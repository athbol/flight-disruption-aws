import iam

ACCOUNT = "123456789012"
REGION = "eu-central-1"
VPS_IP = "203.0.113.7"
TABLE_ARN = f"arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/fda-live"
BUCKET_ARN = "arn:aws:s3:::fda-bucket"
SUBJECT = "repo:athbol@32675046/flight-disruption-aws@1406409508"
SUB = f"{SUBJECT}:ref:refs/heads/main"
OIDC_ARN = f"arn:aws:iam::{ACCOUNT}:oidc-provider/token.actions.githubusercontent.com"


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


def test_consumer_has_no_read_all_or_delete_actions():
    found = all_actions(consumer())
    forbidden = {"dynamodb:DeleteItem", "dynamodb:Scan", "s3:GetObject", "s3:DeleteObject"}
    assert not found & forbidden
    assert {"dynamodb:PutItem", "dynamodb:Query", "s3:PutObject"} <= found


def test_glue_cannot_write_raw_or_artifacts():
    for statement in glue()["Statement"]:
        writes = {"s3:PutObject", "s3:DeleteObject"} & set(actions(statement))
        if writes:
            assert f"{BUCKET_ARN}/raw/*" not in resources(statement)
            assert f"{BUCKET_ARN}/artifacts/*" not in resources(statement)


def test_glue_can_write_curated():
    writes_curated = [
        statement
        for statement in glue()["Statement"]
        if f"{BUCKET_ARN}/curated/*" in resources(statement)
        and {"s3:PutObject", "s3:DeleteObject"} <= set(actions(statement))
    ]
    assert writes_curated


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


def test_deploy_iam_resources_are_all_fda_scoped():
    policy = iam.deploy_policy(ACCOUNT)
    for statement in policy["Statement"]:
        for resource in resources(statement):
            assert resource != "*"
            assert "fda-" in resource or resource == OIDC_ARN, resource


def test_deploy_can_pass_only_fda_roles():
    policy = iam.deploy_policy(ACCOUNT)
    passing = [s for s in policy["Statement"] if "iam:PassRole" in actions(s)]
    assert passing
    for statement in passing:
        assert resources(statement) == [f"arn:aws:iam::{ACCOUNT}:role/fda-*"]
