#!/usr/bin/env python3
import os

import aws_cdk as cdk
from stacks.bronze_stack import BronzeStack
from stacks.silver_stack import SilverStack
from stacks.gold_stack   import GoldStack

app = cdk.App()

env = cdk.Environment(
    account=os.getenv('CDK_DEFAULT_ACCOUNT'),
    region=os.getenv('CDK_DEFAULT_REGION')
)

bronze = BronzeStack(app, "BronzeStack", env = env)

silver = SilverStack(app, "SilverStack", bronze_bucket=bronze.data_lake, env=env)

gold = GoldStack(
    app, "GoldStack",
    bronze_bucket=bronze.data_lake,
    discord_webhook_url=os.getenv("DISCORD_WEBHOOK_URL", ""),
    env=env,
)

app.synth()
