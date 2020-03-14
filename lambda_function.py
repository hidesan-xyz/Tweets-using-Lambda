import boto3
from boto3.dynamodb.conditions import Key, Attr
from requests_oauthlib import OAuth1Session
 
CK = 'Consumer API Key'
CS = 'Consumer API secret key'
AT = 'Access token'
AS = 'Access token secret'
 
URL = 'https://api.twitter.com/1.1/statuses/update.json'
 
def lambda_handler(event, context):
    
    tweet = get_tweet()
    session = OAuth1Session(CK, CS, AT, AS)
 
    params = {"status": tweet }
    session.post(URL, params = params)
    
    
def get_tweet():
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('テーブル名')
    
    items = table.get_item(
            Key={
                 "id": 1
                }
            )
    
    return str(items["Item"]["tweet"])