import json
import boto3
from boto3.dynamodb.conditions import Attr

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

patch_all()

USERS_TO_FULFILL = 70
UPDATE_VERSION = 'v1'

def to_entry(user):
    return {
            'Id': str(user['id']),
            'MessageBody': str(user['id'])
        }
        

def query_users(users_table):
    users = users_table.scan(FilterExpression=Attr('update_state').eq('notified'),
                             ProjectionExpression='id')
    return users['Items'][:USERS_TO_FULFILL]

def fulfill(users_table, sqs_client):
    users = query_users(users_table)
    print(f'Queried {len(users)} out of {USERS_TO_FULFILL}')
    
    
    users = [users[i:i+10] for i in range(0, len(users), 10)]
    
    for user_batch in users:
        response = sqs_client.send_message_batch(
                        QueueUrl='https://sqs.us-east-1.amazonaws.com/757429926343/TestQueue',
                        Entries=list(map(to_entry, user_batch)))
        print('Executed batch')
    
def process():
    db_client = boto3.resource('dynamodb')
    users_table = db_client.Table('nautilus-users')
    sqs_client = boto3.client('sqs')
    fulfill(users_table, sqs_client)

def lambda_handler(event, context):
    process()
    return {
        'statusCode': 200,
        'body': json.dumps('Succuessful Operation')
    }
