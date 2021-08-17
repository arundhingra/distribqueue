import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

from botocore.signers import CloudFrontSigner
import rsa
from datetime import datetime

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

import base64
from botocore.exceptions import ClientError
            

patch_all()

client = boto3.resource('dynamodb')
users_table = client.Table('nautilus-users')
mgmt_table = client.Table('update_mgmt')


def get_secret():

    secret_name = "distrib-priv-key"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    

    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    
    secret = json.loads(get_secret_value_response['SecretString'], strict=False)
    return secret

def update_users(id, tier, key):
    users_table.update_item(
        Key= {'id': id, 'tier': tier},
        UpdateExpression= 'set update_state=:g, last_update=:f',
        ExpressionAttributeValues= {':g': 'fulfilled', ':f': key},
        ReturnValues="UPDATED_NEW"
    )
    
def update_mgmt_batch(id, users):
    key = get_key(id)
    print(f'Marking {users} user as fulfilled')
    mgmt_table.update_item(
        Key= {'update_version': key},
        UpdateExpression= 'ADD fulfilled_users :g ',
        ExpressionAttributeValues= {':g': users},
        ReturnValues="UPDATED_NEW"
    )
    
def update_mgmt(key):
    print(f'Marking 1 user as fulfilled')
    mgmt_table.update_item(
        Key= {'update_version': key},
        UpdateExpression= 'ADD fulfilled_users :g ',
        ExpressionAttributeValues= {':g': 1},
        ReturnValues="UPDATED_NEW"
    )
    
    
def rsa_signer(message):
    print('Getting key')
    private_key = get_secret()
    private_key = private_key['PRIV_KEY']
    return rsa.sign(
        message,
        rsa.PrivateKey.load_pkcs1(private_key.encode('utf8')),
        'SHA-1')  # CloudFront requires SHA-1 hash


    
def signed_url(key):
    print('Generating Signed-URL')
    cf_signer = CloudFrontSigner('KMW14LRL66EH8', rsa_signer)
    
    print('Signed', cf_signer)

    # To sign with a canned policy::
    signed_url = cf_signer.generate_presigned_url('https://d23ehmpvbhpefp.cloudfront.net/' + key, date_less_than=datetime(2022, 12, 1))

    return signed_url

def get_tier(id):
    users = users_table.scan(FilterExpression=Attr('id').eq(id))
    return users['Items'][0]['tier']

def get_key(id):
    users = users_table.scan(FilterExpression=Attr('id').eq(id))
    return users['Items'][0]['latest_update']

def process(id, batch, batch_size):
    key = get_key(id)
    
    update_users(id, get_tier(id), key)
    print('User table updated')
    
    if (not batch):
        update_mgmt(key)
        print('Management table updated')
        return signed_url(key)
        
def metric(users):
    client = boto3.client('cloudwatch')
    resp = client.put_metric_data(
            Namespace='Nautilus',
            MetricData=[
                {
                    'MetricName': 'Fulfilled Users',
                    'Value': users,
                },
            ]
        )
    print(resp)
    
def error(id):
    sqs_client = boto3.client('sqs')
    response = sqs_client.send_message(
                        QueueUrl='https://sqs.us-east-1.amazonaws.com/757429926343/nautilus-error',
                        MessageBody=str(id))
    print(response)
    return {'statusCode': 500}
    


def lambda_handler(event, context):
    
    if 'Records' in event:
        print('Batch Process')
        users = len(event['Records'])
        
        for record in event['Records']:
            id = int(record['body'])
            process(id, True, users)
        
        print('Management table updated') 
        update_mgmt_batch(id, users)
        metric(users)
        
        return {'statusCode': 200, 'desc': 'Successful Operation'}
    else:
        print('Regular Process')
        if event['error'] == 'true':
            return error(int(event['id']))
        else:
            url = process(int(event['id']), False, 1)
            metric(1)
            return url
