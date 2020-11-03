# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import mws
import csv
import re
import boto3

from Config import config
from RequestReport import TABLE_REQUEST_STATE

dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def dynamo_init():
    # java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb
    try:
        client_keys = dynamodb.create_table(
            TableName="client_keys",
            AttributeDefinitions=[
                {
                    "AttributeName": "pair_id",
                    "AttributeType": "N"
                }
            ],
            KeySchema=[
                {
                    "AttributeName": "pair_id",
                    "KeyType": "HASH"
                }
            ],
            ProvisionedThroughput={
                "NumberOfDecreasesToday": 0,
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5
            }
        )
        client_keys.wait_until_exists()

        developer_keys = dynamodb.create_table(
            TableName="developer_keys",
            AttributeDefinitions=[
                {
                    "AttributeName": "developer_id",
                    "AttributeType": "S"
                }
            ],
            KeySchema=[
                {
                    "AttributeName": "developer_id",
                    "KeyType": "HASH"
                }
            ],
            ProvisionedThroughput={
                "NumberOfDecreasesToday": 0,
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5
            },
        )
        developer_keys.wait_until_exists()
        # merchants = dynamodb.create_table(
        #     TableName='merchants',
        #     KeySchema=[
        #         {
        #             'AttributeName': 'merchant_id',
        #             'KeyType': 'HASH'
        #         },
        #     ],
        #     AttributeDefinitions=[
        #         {
        #             'AttributeName': 'merchant_id',
        #             'AttributeType': 'S'
        #         },
        #     ],
        #     ProvisionedThroughput={
        #         'ReadCapacityUnits': 10,
        #         'WriteCapacityUnits': 10
        #     }
        # )
        # merchants.wait_until_exists()
    except Exception as ex:
        print('Create Merchant Table {}'.format(ex))

    try:
        state_table = dynamodb.create_table(
            TableName=TABLE_REQUEST_STATE,
            KeySchema=[
                {
                    'AttributeName': 'request_id',
                    'KeyType': 'HASH'
                },
            ],
            GlobalSecondaryIndexes=[{
                "IndexName": "merchant_id-index",
                "Projection": {
                    "ProjectionType": "ALL"
                },
                "ProvisionedThroughput": {
                    "WriteCapacityUnits": 5,
                    "ReadCapacityUnits": 10
                },
                "KeySchema": [{
                    "KeyType": "HASH",
                    "AttributeName": "merchant_id"
                },
                {
                    'AttributeName': 'end_date',
                    'KeyType': 'RANGE'
                }],
            }],
            AttributeDefinitions=[
                {
                    'AttributeName': 'request_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'merchant_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'end_date',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        state_table.wait_until_exists()
    except Exception as ex:
        print('Create RequestState Table {}'.format(ex))

    try:
        history_table = dynamodb.create_table(
            TableName='request_history',
            KeySchema=[
                {
                    'AttributeName': 'request_id',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'merchant_id',
                    'KeyType': 'RANGE'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'request_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'merchant_id',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        history_table.wait_until_exists()
    except Exception as ex:
        print('Create History Table {}'.format(ex))


def get_merchant_list():
    # For a Boto3 client.
    # response = ddb.list_tables()
    # print(response)

    # if not dynamodb:
    # dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('merchants')
    params = {'Select': "ALL_ATTRIBUTES" }
    response = table.scan(**params)
    # print(response)
    # if response['Count'] > 0:
    #     return response['Items']

    return response

reports_api = mws.Reports(access_key=config['access_key'], secret_key=config['secret_key'],
                          account_id=config['merchant_id'], region='US', auth_token=config['mws_auth_token'])


def get_reports_api(merchant_info):
    reports_api = mws.Reports(access_key=config['access_key'], secret_key=config['secret_key'],
                             account_id=merchant_info['merchant_id'],
                             region=merchant_info['market_place'],
                             auth_token=merchant_info['auth_token'])
    return reports_api


def get_report_id(report_type_str):

    _list = reports_api.get_report_list()
    if _list.response.status_code == 200:
        report_id = None
        for report_type in _list.parsed['ReportInfo']:
            if report_type['ReportType']['value'] == report_type_str:
                report_id = report_type['ReportId']['value']
                break
        return report_id
    return None


def get_report(report_id):

    report = reports_api.get_report(report_id)

    # Split the long string into a list of lines
    data = report.original.decode('utf-8').splitlines()

    # Open the file for writing
    with open("./1.csv", "w") as csv_file:
        # Create the writer object with tab delimiter
        writer = csv.writer(csv_file, delimiter='\t')
        for line in data:
            # Writerow() needs a list of data to be written, so split at all empty spaces in the line
            writer.writerow(re.split('\s+', line))


def get_last_request(merchant):
    table = dynamodb.Table('merchants')
    params = {'Select': "ALL_ATTRIBUTES"}
    response = table.scan(**params)
    # print(response)
    # if response['Count'] > 0:
    #     return response['Items']
    return response


# def init_request():
#     merchants = get_merchant_list()
#     if merchants['Count'] > 0:
#         for merchant in merchants['Items']:
#             get_last_request(merchant)
#             merchant_api = mws.Reports(access_key=config['access_key'], secret_key=config['secret_key'],
#                                        account_id=merchant['merchant_id'], region=merchant['market_place'],
#                                        auth_token=merchant['auth_token'])
#             merchant_api.request_report(report_type='_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_',
#                                         start_date='',
#                                         end_date='')

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    dynamo_init()

    get_merchant_list()
    report_id = get_report_id('_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_')
    get_report(report_id)
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
