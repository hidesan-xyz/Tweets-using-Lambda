# coding:utf-8

import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
from requests_oauthlib import OAuth1Session

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
 
CK = os.environ['Consumer_API_Key']
CS = os.environ['Consumer_API_secret_key']
AT = os.environ['Access_token']
AS = os.environ['Access_token_secret']
table_name = os.environ['Table_name']
 
URL = 'https://api.twitter.com/1.1/statuses/update.json'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(table_name)
 
def lambda_handler(event, context):
    
    target_record = get_tweet()
    tweet = target_record['text']

    # debug
    # logger.info(tweet)
    # logger.info(target_record)

    session = OAuth1Session(CK, CS, AT, AS)
 
    params = {"status": tweet }
    session.post(URL, params = params)

    update_data(target_record)
    
    
def get_tweet():
    
    target_record = {'no':999999,'text':'last','update_date_and_time':99999999999999}

    # 最終更新日が0のレコードを抽出
    items = table.query(
        IndexName='update_date_and_time-index',
        KeyConditionExpression=Key('update_date_and_time').eq(0)
    )

    if items['Count'] == 0:
        # 全てのレコードを抽出して、最終更新日が一番古いレコードを選択
        data_set = table.scan()

        for data in data_set['Items']:
            if data['update_date_and_time'] < target_record['update_date_and_time']:
                target_record = data
    else:
        # 最終更新日が0の中でNoが一番小さいレコードを選択
        for data in items['Items']:
            if data['no'] < target_record['no']:
                target_record = data

    return target_record


def update_data(target_record):

    # 今の日時を数値型として取得(yyyymmddHHMM)
    now = int(datetime.now().strftime("%Y%m%d%H%M"))

    res = table.update_item(
        Key = {'no': target_record['no']},
        UpdateExpression = 'set ' + 'update_date_and_time' + '=:' + 'update_date_and_time',
        ExpressionAttributeValues = {
                ':' + 'update_date_and_time' : now
        },
        ReturnValues="UPDATED_NEW"
    )

