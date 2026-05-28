import aws_cdk as core
import aws_cdk.assertions as assertions

from stacks.bronze_stack import BronzeStack

# example tests. To run these tests, uncomment this file along with the example
# resource in stacks/bronze_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = BronzeStack(app, "BronzeStack")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
