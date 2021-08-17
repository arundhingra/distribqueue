import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all


patch_all()

batch_update = 100

def unwrap_email(el):
    return el['email']

def unwrap_id(el):
    return int(el['id'])
    
def unwrap_tier(el):
    return el['tier']
    
def send(client, email_lst, key):
    email_content = json.load(open('email.json'))
    
    email_content['Simple']['Body']['Text']['Data'] = email_content['Simple']['Body']['Text']['Data'].replace("UPDATE_VERSION", key)
    email_content['Simple']['Body']['Html']['Data'] = email_content['Simple']['Body']['Html']['Data'].replace("UPDATE_VERSION", key)
    
    response = client.send_email(FromEmailAddress='arunsdhingra@gmail.com',
                          Destination={
                              'ToAddresses': email_lst
                          },
                            Content=email_content)
    print(response)
    
def update_users(user_table, id_lst, tier_lst, key):
    for id, tier in zip(id_lst, tier_lst):
        user_table.update_item(
            Key= {'id': id, 'tier': tier},
            UpdateExpression= 'set update_state=:g, latest_update=:f',
            ExpressionAttributeValues= {':g': 'notified', ':f': key},
            ReturnValues="UPDATED_NEW"
        )

def notify(user_table, mgmt_table):
    users = user_table.query(
                       KeyConditionExpression=Key('tier').eq('platinum'),
                       FilterExpression=Attr('update_state').eq('unnotified'),
                       ProjectionExpression='id,email,tier,latest_update')['Items']
                       
    print(f'Queried {len(users)} users')
    users = users[:batch_update]
    email_lst = list(map(unwrap_email, users))
    id_lst = list(map(unwrap_id, users))
    tier_lst = list(map(unwrap_tier, users))
    print(tier_lst)
    key = users[0]['latest_update']
    
    send(boto3.client('sesv2'), email_lst[:1], key)
    print('Users notified')
    
    update_users(user_table, id_lst, tier_lst, key)
    print('Users updated')
    
    return {
        'status': 200
    }
    
def set_unnotified(user_table, key):
    users = user_table.scan(ProjectionExpression='id,tier')['Items']
    
    id_lst = list(map(unwrap_id, users))
    tier_lst = list(map(unwrap_tier, users))
    
    for id, tier in zip(id_lst, tier_lst):
        user_table.update_item(
            Key= {'id': id, 'tier': tier},
            UpdateExpression= 'set update_state=:g, latest_update=:f',
            ExpressionAttributeValues= {':g': 'unnotified', ':f': key},
            ReturnValues="UPDATED_NEW"
        )
        
        
def create_update(mgmt_table, key):
    mgmt_table.put_item(
        Item={
            'update_version': key,
            'notified_users': 100,
            'fulfilled_users': 0
        }
    )

def process(event):
    db_client = boto3.resource('dynamodb')
    user_table = db_client.Table('nautilus-users')
    mgmt_table = db_client.Table('update_mgmt')
    key = event['Records'][0]['s3']['object']['key']
    
    set_unnotified(user_table, key)
    create_update(mgmt_table, key)
    print('Reset update state')
    return notify(user_table, mgmt_table)
    
def lambda_handler(event, context):
    return process(event)
    
