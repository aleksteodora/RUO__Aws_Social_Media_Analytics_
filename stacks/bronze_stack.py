from aws_cdk import (
    Stack,
    Duration,
    aws_s3          as s3,
    aws_lambda      as _lambda,
    aws_iam         as iam,
    aws_events      as events,
    aws_events_targets as targets,
    RemovalPolicy,
)
from constructs import Construct

class BronzeStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        data_lake = s3.Bucket(
            self, "DataLakeBucket",
            bucket_name="social-media-data-lake",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        lambda_role = iam.Role(
            self, "HNCollectorRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Minimal role for HN bronze collector Lambda",
        )

        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:PutObject"],
                resources=[f"{data_lake.bucket_arn}/bronze/*"],
            )
        )

        hn_collector = _lambda.Function(
            self, "HNCollectorLambda",
            function_name="hn-bronze-collector",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(
                "lambdas/hacker_news"
            ),
            role=lambda_role,
            timeout=Duration.minutes(10),
            memory_size=256,
            environment={
                "BRONZE_BUCKET_NAME": data_lake.bucket_name,
            },
        )

        daily_trigger = events.Rule(
            self, "DailyHNTrigger",
            rule_name="daily-hn-collection",
            description="Triggers HN bronze collector every day at midnight UTC",
            schedule=events.Schedule.cron(
                minute="0",
                hour="0",
                day="*",
                month="*",
                year="*",
            ),
        )
        daily_trigger.add_target(
            targets.LambdaFunction(hn_collector)
        )
