from aws_cdk import (
    Stack,
    Duration,
    aws_lambda     as _lambda,
    aws_s3         as s3,
    aws_iam        as iam,
    aws_events     as events,
    aws_events_targets as targets,
)
from constructs import Construct


AWSWRANGLER_LAYER_ARN = (
    "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:16"
)


class GoldStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        bronze_bucket: s3.Bucket,
        discord_webhook_url: str = "",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        gold_role = iam.Role(
            self,
            "GoldLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Minimal role for Gold Lambda - reads silver, writes gold",
        )

        gold_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        )

        gold_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    bronze_bucket.bucket_arn,
                    f"{bronze_bucket.bucket_arn}/silver/*",
                ],
            )
        )

        gold_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject", "s3:DeleteObject"],
                resources=[
                    f"{bronze_bucket.bucket_arn}/gold/*",
                ],
            )
        )

        wrangler_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "AwsWranglerLayer",
            AWSWRANGLER_LAYER_ARN,
        )

        gold_fn = _lambda.Function(
            self,
            "GoldProcessor",
            function_name="hn-gold-processor",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.process",
            code=_lambda.Code.from_asset("lambdas/gold"),
            memory_size=512,
            timeout=Duration.minutes(10),
            role=gold_role,
            layers=[wrangler_layer],
            environment={
                "BUCKET_NAME":         bronze_bucket.bucket_name,
                "SILVER_PREFIX":       "silver",
                "GOLD_PREFIX":         "gold",
                "DISCORD_WEBHOOK_URL": discord_webhook_url,
            },
        )

        rule = events.Rule(
            self,
            "SilverToGoldTrigger",
            rule_name="silver-to-gold-trigger",
            description="Triggers Gold Lambda when Silver writes posts parquet to S3",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [bronze_bucket.bucket_name]},
                    "object": {"key": [{"prefix": "silver/posts/"}]},
                },
            ),
        )
        rule.add_target(targets.LambdaFunction(gold_fn))

        self.gold_fn = gold_fn