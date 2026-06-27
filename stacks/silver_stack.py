from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct


AWSWRANGLER_LAYER_ARN = (
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:16"
)


class SilverStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, bronze_bucket: s3.Bucket, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        silver_role = iam.Role(
            self,
            "SilverLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Minimal role for Silver Lambda - reads bronze, writes silver",
        )

        silver_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        )

        silver_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    bronze_bucket.bucket_arn,
                    f"{bronze_bucket.bucket_arn}/bronze/*",
                ],
            )
        )

        silver_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject", "s3:DeleteObject"],
                resources=[
                    f"{bronze_bucket.bucket_arn}/silver/*",
                ],
            )
        )

        wrangler_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "AwsWranglerLayer",
            AWSWRANGLER_LAYER_ARN,
        )

        silver_fn = _lambda.Function(
            self,
            "SilverProcessor",
            function_name="hn-silver-processor",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.process",
            code=_lambda.Code.from_asset("lambdas/silver"),
            memory_size=512,
            timeout=Duration.minutes(15),
            role=silver_role,
            layers=[wrangler_layer],
            environment={
                "BUCKET_NAME": bronze_bucket.bucket_name,
                "BRONZE_HN_PREFIX": "bronze/hacker-news",
                "BRONZE_TWITTER_KEY": "bronze/twitter/sentiment140.csv",
                "SILVER_PREFIX": "silver",
            },
        )

        rule = events.Rule(
            self,
            "BronzeToSilverTrigger",
            rule_name="bronze-to-silver-trigger",
            description="Triggers Silver Lambda when HN bronze Lambda writes data to S3",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [bronze_bucket.bucket_name]},
                    "object": {"key": [{"prefix": "bronze/hacker-news/"}]},
                },
            ),
        )
        rule.add_target(targets.LambdaFunction(silver_fn))