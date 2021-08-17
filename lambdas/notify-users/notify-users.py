import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

patch_all()
batch_update = 100
notified = False

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

def update_users(user_table, id_lst, tier_lst, key):
    for id, tier in zip(id_lst, tier_lst):
        user_table.update_item(
            Key= {'id': id, 'tier': tier},
            UpdateExpression= 'set update_state=:g, latest_update=:f',
            ExpressionAttributeValues= {':g': 'notified', ':f': key},
            ReturnValues="UPDATED_NEW"
        )
        
def query_users(user_table, tier):
    return user_table.query(
                           KeyConditionExpression=Key('tier').eq(tier),
                           FilterExpression=Attr('update_state').eq('unnotified'),
                           ProjectionExpression='id,email,tier')['Items']
                           
def check_load(key, mgmt_table):
    entry = mgmt_table.query(KeyConditionExpression=Key('update_version').eq(key))['Items'][0]
    return (entry['fulfilled_users'] / entry['notified_users']) >= .7
    
def get_users(user_table):
    users = query_users(user_table, 'platinum')
    if (len(users) == 0):
        users = query_users(user_table, 'premium')
        if (len(users) == 0):
            users = query_users(user_table, 'base')
    return users[:batch_update]
    
def update_mgmt(mgmt_table, key, users):
    mgmt_table.update_item(
        Key= {'update_version': key},
        UpdateExpression= 'ADD notified_users :g ',
        ExpressionAttributeValues= {':g': users},
        ReturnValues="UPDATED_NEW"
    )
    

def notify_users(key, user_table, mgmt_table):
    users = get_users(user_table)
                       
    print(f'Queried {len(users)} users')
    
    if len(users) != 0:
        email_lst = list(map(unwrap_email, users))
        id_lst = list(map(unwrap_id, users))
        tier_lst = list(map(unwrap_tier, users))
        
        send(boto3.client('sesv2'), email_lst[:1], key)
        print('Users notified')
        
        update_users(user_table, id_lst, tier_lst, key)
        print('User table updated')
        update_mgmt(mgmt_table, key, len(users))
        print('Management table updated')
    
    return {
        'status': 200
    }

def intify(n):
    return int(n['N'])
    
    
def process(old_blob, new_blob):
    load = intify(new_blob['fulfilled_users']) / intify(new_blob['notified_users'])

    if intify(old_blob['fulfilled_users']) < intify(new_blob['fulfilled_users']) and load > 1:
        print(f'{load} should be impossible, no users notified')
    elif intify(old_blob['fulfilled_users']) < intify(new_blob['fulfilled_users']) and load >= .7:
        db_client = boto3.resource('dynamodb')
        user_table = db_client.Table('nautilus-users')
        mgmt_table = db_client.Table('update_mgmt')
        key = old_blob['update_version']['S']
        check_load(key, mgmt_table)
        if (check_load(key, mgmt_table)):
            notify_users(key, user_table, mgmt_table)
            metric(batch_update)
            print(f'Load at {load}, notified users')
            notified = True
        else:
            print('No notification sent.')
    else:
        print(f'No notification sent, load was at {load}')
    
    
def record_iter(event):
    for record in event['Records']:
        if record['eventName'] == 'MODIFY' and not notified:
            process(record['dynamodb']['OldImage'],
                    record['dynamodb']['NewImage'])
            
            

def metric(users):
    client = boto3.client('cloudwatch')
    resp = client.put_metric_data(
            Namespace='Nautilus',
            MetricData=[
                {
                    'MetricName': 'Notified Users',
                    'Value': users,
                },
            ]
        )

def lambda_handler(event, context):
    print("Begin Event\n")
    
    record_iter(event)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
