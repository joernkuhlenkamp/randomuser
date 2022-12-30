from aws_cdk import (   aws_events as events,
                        aws_events_targets,
                        aws_iam as iam,
                        aws_lambda,
                        aws_s3 as s3,
                        Duration,
                        CfnResource,
                        CfnOutput,
                        Stack, )
from constructs import Construct

class RandomuserConnectorStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        requests_layer = aws_lambda.LayerVersion.from_layer_version_arn(
            scope=self,
            id='requests_layer',
            layer_version_arn='arn:aws:lambda:eu-central-1:770693421928:layer:Klayers-p39-requests:8'
        )

        function = aws_lambda.Function(
            scope=self,
            id="etl", 
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            handler="handler.main",
            code=aws_lambda.Code.from_asset("./lambda/src"),
            layers=[ requests_layer ]
        )

        cfn_url = aws_lambda.CfnUrl(
            scope=self,
            id="MyCfnUrl",
            auth_type="NONE",
            target_function_arn=function.function_arn,
        )
        
        function_permission = CfnResource(
            scope=self,
            id="lambdaPermission", 
            type="AWS::Lambda::Permission",
            properties={
                'Action': "lambda:InvokeFunctionUrl",
                'FunctionName': function.function_name,
                'Principal': "*",
                'FunctionUrlAuthType': "NONE",
            },
        )

        event_schedule = events.Schedule.rate(Duration.days(1))

        event_target = aws_events_targets.LambdaFunction(handler=function)

        lambda_cw_event = events.Rule(
            scope=self,
            id="dailyETLTrigger",
            description="Trigger ETL job once per day.",
            enabled=True,
            schedule=event_schedule,
            targets=[event_target]
        )

        bucket = s3.Bucket(
            self,
            "query_storage"
        )
        bucket.grant_write(function)

        bucket.add_to_resource_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=['s3:GetObject'],
                resources=[
                    f'arn:aws:s3:::{bucket.bucket_name}/users.json'
                ],
                principals=[iam.ArnPrincipal('*')]
            )
        )

        function.add_environment('BUCKET_NAME', bucket.bucket_name)
        function.add_environment('FILE_NAME', 'users.json')

        CfnOutput(
            scope=self,
            id="trigger_etl_job",
            value=cfn_url.get_att(attribute_name="FunctionUrl").to_string(),
        )
        CfnOutput(
            scope=self,
            id="get_users",
            value=bucket.virtual_hosted_url_for_object("users.json"),
        )
        CfnOutput(
            scope=self,
            id="s3bucket",
            value=bucket.bucket_name,
        )