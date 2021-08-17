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

dynamo_client = boto3.resource('dynamodb')
users_table = dynamo_client.Table('nautilus-users')
mgmt_table = dynamo_client.Table('update_mgmt')

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

def send(key, url):
    ses_client = boto3.client('sesv2')
    email_content = json.load(open('email.json'))
    
    email_content['Simple']['Subject']['Data'] = email_content['Simple']['Subject']['Data'].replace("UPDATE_VERSION", key)
    email_content['Simple']['Body']['Text']['Data'] = email_content['Simple']['Body']['Text']['Data'].replace("UPDATE_VERSION", key)
    email_content['Simple']['Body']['Html']['Data'] = email_content['Simple']['Body']['Html']['Data'].replace("UPDATE_VERSION", key)
    email_content['Simple']['Body']['Text']['Data'] = email_content['Simple']['Body']['Text']['Data'].replace("DOWNLOAD_URL", url)
    email_content['Simple']['Body']['Html']['Data'] = email_content['Simple']['Body']['Html']['Data'].replace("DOWNLOAD_URL", url)
    
    response = ses_client.send_email(FromEmailAddress='arunsdhingra@gmail.com',
                          Destination={
                              'ToAddresses': ['arunsdhingra@gmail.com']
                          },
                            Content=email_content)
    print(response)

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
    url = boto3.client('s3').generate_presigned_url(
        ClientMethod='get_object', 
        Params={'Bucket': 'nautilus-update-store', 'Key': key},
        ExpiresIn=3600)
        
    cf_signer = CloudFrontSigner('KMW14LRL66EH8', rsa_signer)
    
    # To sign with a canned policy::
    signed_url = cf_signer.generate_presigned_url('https://d23ehmpvbhpefp.cloudfront.net/' + key, date_less_than=datetime(2022, 12, 1))

    return signed_url

def get_tier(id):
    users = users_table.scan(FilterExpression=Attr('id').eq(id))
    return users['Items'][0]['tier']

def get_key(id):
    users = users_table.scan(FilterExpression=Attr('id').eq(id))
    return users['Items'][0]['latest_update']

def process(id):
    key = get_key(id)
    
    update_users(id, get_tier(id), key)
    print('User table updated')
    
    update_mgmt(key)
    print('Management table updated')
    send(key, signed_url(key))
    print('Email Sent')
    
def error(id):
    tier = get_tier(id)
    sqs_client = boto3.client('sqs')
    response = sqs_client.send_message(
                        QueueUrl='https://sqs.us-east-1.amazonaws.com/757429926343/nautilus-error',
                        MessageBody=str(id) + tier)
    print(response)
    return {'statusCode': 500}
    


def lambda_handler(event, context):
    
    for record in event['Records']:
        id = int(record['body'])
        process(id)

    return {'statusCode': 200, 'desc': 'Successful Operation'}

