#!/usr/bin/env python3
import os

import aws_cdk as cdk
from stacks.bronze_stack import BronzeStack

app = cdk.App()

BronzeStack(app, "BronzeStack",
    env = cdk.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=os.getenv('CDK_DEFAULT_REGION')
    ),
)

app.synth()
