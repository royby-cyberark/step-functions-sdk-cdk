from aws_cdk import core as cdk
from aws_cdk import (
    aws_s3 as s3, 
    aws_dynamodb as dynamodb, 
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_iam as iam,
)

# For consistency with other languages, `cdk` is the preferred import name for
# the CDK's core module.  The following line also imports it as `core` for use
# with examples from the CDK Developer's Guide, which are in the process of
# being updated to use `cdk`.  You may delete this import if you don't need it.
from aws_cdk import core


class StepFunctionSdkStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        bucket = s3.Bucket(
            scope=self, 
            id="StepFunctionSdkTestBucket", 
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
        )

        table = dynamodb.Table(
            scope=self, 
            id="StepFunctionSdkTestTable", 
            partition_key=dynamodb.Attribute(name='id', type=dynamodb.AttributeType.STRING)
        )


        get_s3_object = sfn_tasks.CallAwsService(
            scope=self, 
            id="SfSdkCallGetObject",
            service='s3',
            action='getObject',
            parameters={
                'Bucket': bucket.bucket_name,
                'Key': sfn.JsonPath.string_at('$.key')
            },
            iam_resources=[bucket.arn_for_objects('*')],
        )


        put_ddb_item = sfn_tasks.CallAwsService(
            scope=self, 
            id="SfSdkCallPutItem",
            service='dynamodb',
            action='putItem',
            parameters={
                'TableName': table.table_name,
                'Item':  {
                    "id": {
                        "S": "2"
                    },
                    "data": {
                        "S.$": "States.JsonToString($.Body)"
                    }
                }
            },
            iam_resources=['*'],
        )

        get_s3_object.next(put_ddb_item)

        sfn_role = iam.Role(
            self, "SfnSdkTestRole",
            assumed_by=iam.ServicePrincipal('states.amazonaws.com'),
            role_name="SfnSdkTestRole"
        )

        bucket.grant_read(sfn_role)
        table.grant_read_write_data(sfn_role)

        sfn.StateMachine(
            scope=self, 
            id="SdkTestStateMachine",
            timeout=core.Duration.minutes(5),
            definition=get_s3_object,
            role=sfn_role
        )

