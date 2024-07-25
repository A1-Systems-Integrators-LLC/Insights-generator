import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_apigateway as apigateway,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_iam as iam,
)
from constructs import Construct
from aws_cdk import Duration

class Architecture(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an S3 bucket
        bucket = s3.Bucket(self, "Bucket")

        insights_verification = lambda_.Function(
            self, 'InsightsVerification',
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler='verify_insights.verify_insights',
            code=lambda_.Code.from_docker_build(
                "lambda/insights_verification",
        ),
        timeout=Duration.seconds(900)
        )
        # Create consolidated Lambda function
        insights_generation = lambda_.Function(
            self, 'InsightsGeneration',
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler='main.main',
            code=lambda_.Code.from_docker_build(
                "lambda/insights_generation",
        ),
        timeout=Duration.seconds(900),
        environment={
            "InsightsVerificationLambdaFunctionName": insights_verification.function_name,
            "S3_BUCKET_NAME": bucket.bucket_name
            },
             
        )

        api_lambda = lambda_.Function(
            self, "api",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="api_function.handler",
            code=lambda_.Code.from_asset("lambda/api_gateway"),
            environment={
                "S3_BUCKET_NAME": bucket.bucket_name
            },
            timeout=Duration.seconds(100)
        )

        # Grant Lambda function access to S3 bucket
        bucket.grant_read_write(insights_generation)
        bucket.grant_read(api_lambda)

        # Add S3 event notification to trigger Lambda
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(insights_generation),
        )


        # Create API Gateway
        api = apigateway.RestApi(self, "ApiGateway")

        # Create API Gateway integration with Lambda function
        lambda_integration = apigateway.LambdaIntegration(api_lambda)
        api.root.add_method("POST", lambda_integration)
        api.root.add_method("GET", lambda_integration)

        # Add permissions for invoking Bedrock
        bedrock_policy = iam.PolicyStatement(
            actions=["bedrock:InvokeModel","bedrock:ListFoundationModels"],
            resources=["*"]  # Specify the ARN of your Bedrock model if possible
        )
        guardrail_policy = iam.PolicyStatement(
            actions=[  
                    "bedrock:*",
                ],
            resources=["*"]
        )
        insights_generation.add_to_role_policy(guardrail_policy)
        insights_generation.add_to_role_policy(bedrock_policy)
        insights_verification.add_to_role_policy(bedrock_policy)
        insights_generation.role.add_to_policy(iam.PolicyStatement(
        actions=["lambda:InvokeFunction"],
        resources=[insights_verification.function_arn]
        ))
        lambda_.EventInvokeConfig(
        self, "api_InvokeConfig",
        function=insights_generation,
        retry_attempts=0
        )       
app = cdk.App()
Architecture(app, "Architecture")
app.synth()