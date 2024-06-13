import os
import time
import boto3
from botocore.config import Config
from datetime import datetime
import sys

DYNAMODB_REGION = os.environ['INPUT_AWS_REGION']
DYNAMODB_SOURCE_TABLE = os.environ['INPUT_DYNAMODB_SOURCE_TABLE']
DYNAMODB_DESTINATION_TABLE = os.environ['INPUT_DYNAMODB_DESTINATION_TABLE']

configuration = Config(region_name=DYNAMODB_REGION)
dynamodb_client = boto3.client('dynamodb',config=configuration)

def backup_dynamodb_table(table_name):
    current_date = datetime.now().strftime("%Y%m%d")
    current_time = datetime.now().strftime("%H%M")
    response = dynamodb_client.create_backup(TableName=table_name,BackupName=f"{table_name}_{current_date}_{current_time}")

    backup_arn = response['BackupDetails']['BackupArn']
    while True:
        backup_status = dynamodb_client.describe_backup(BackupArn=backup_arn)['BackupDescription']['BackupDetails']['BackupStatus']
        if backup_status == 'AVAILABLE':
            print(f"Backup completed successfully for table {table_name}.")
            break
        else:
            print(f"Backup in progress for table {table_name}.")
            time.sleep(10)

def truncate_dynamodb_table(table_name):
    dynamo = boto3.resource('dynamodb',region_name=DYNAMODB_REGION)
    table = dynamo.Table(table_name)
    
    #get the table keys
    tableKeyNames = [key.get("AttributeName") for key in table.key_schema]

    #Only retrieve the keys for each item in the table (minimize data transfer)
    projectionExpression = ", ".join('#' + key for key in tableKeyNames)
    expressionAttrNames = {'#'+key: key for key in tableKeyNames}
    
    counter = 0
    page = table.scan(ProjectionExpression=projectionExpression, ExpressionAttributeNames=expressionAttrNames)
    with table.batch_writer() as batch:
        while page["Count"] > 0:
            counter += page["Count"]
            # Delete items in batches
            for itemKeys in page["Items"]:
                batch.delete_item(Key=itemKeys)
            # Fetch the next page
            if 'LastEvaluatedKey' in page:
                page = table.scan(
                    ProjectionExpression=projectionExpression, ExpressionAttributeNames=expressionAttrNames,
                    ExclusiveStartKey=page['LastEvaluatedKey'])
            else:
                break
    print(f"Deleted {counter} items from table {table_name}.")

def copy_dynamodb_table(source_table_name, target_table_name):
    aws_session = boto3.Session()
    aws_account_dynamodb = aws_session.client('dynamodb',region_name=DYNAMODB_REGION)

    dynamo_paginator = aws_account_dynamodb.get_paginator('scan')
    dynamo_response = dynamo_paginator.paginate(
        TableName=source_table_name,
        Select='ALL_ATTRIBUTES',
        ReturnConsumedCapacity='NONE',
        ConsistentRead=True
    )
    for page in dynamo_response:
        for item in page['Items']:
            aws_account_dynamodb.put_item(
                TableName=target_table_name,
                Item=item
            )
    print(f"Successfully copied data from {source_table_name} to {target_table_name}.")

if __name__ == '__main__':
    backup_dynamodb_table(DYNAMODB_DESTINATION_TABLE)
    truncate_dynamodb_table(DYNAMODB_DESTINATION_TABLE)
    copy_dynamodb_table(DYNAMODB_SOURCE_TABLE,DYNAMODB_DESTINATION_TABLE)

