import time
import os
import mws
import boto3
import csv
import re
from boto3.dynamodb.conditions import Key, Attr
from botocore.config import Config

from Config import config

TABLE_REQUEST_STATE = "mws_reports"
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
mws_reports = dynamodb.Table(TABLE_REQUEST_STATE)

# mws_reports_name = os.environ["MWS_REPORTS"]
# dynamodb = boto3.resource('dynamodb')
# mws_reports = dynamodb.Table(mws_reports_name)


class GetReport:

    def __init__(self, account):
        self.reports = []
        self.report = None
        self.account = account
        self.reportClient = mws.Reports(
            account_id=account['merchant_id'],
            access_key=account['access_key'],
            secret_key=account['secret_key'],
            region=account['marketplace'],
            auth_token=account['mws_access_token']
        )
        pass

    def get_report_state(self):
        response = mws_reports.query(
            # _SUBMITTED_
            # _IN_PROGRESS_
            IndexName="pair_id-end_date-index",
            KeyConditionExpression=Key("pair_id").eq(self.account["pair_id"]),
            FilterExpression=Attr('mws_report_status').eq("_SUBMITTED_") | Attr('mws_report_status').eq("_IN_PROGRESS_"),
            ScanIndexForward=False
        )
        if response['Count'] > 0:
            id_list = ()
            #For MWS api limit rate, here should only access one report at a time
            # _info_merchant = response['Items'][0]
            # id_list = id_list + (_info_merchant["request_id"],)
            for _info_merchant in response['Items']:
                id_list = id_list + (_info_merchant["request_id"],)
            self.main_report_request(request_id=id_list)

    def main_report_request(self, request_id):
        print("main_report_request==0 ", request_id)
        rrl_response = self.reportClient.get_report_request_list(requestids=request_id, processingstatuses=('_DONE_'))
        print(rrl_response)
        time.sleep(3)
        try:
            reportInfo_items = rrl_response.parsed['ReportRequestInfo']
            print("main_report_request==1 ", type(reportInfo_items))
            print(reportInfo_items)
            if reportInfo_items is None:
                return
        except Exception as e:
            print("Parsing get_report_request_list exception")
            print(e)
            return
        reportInto_list = []
        if isinstance(reportInfo_items, list):
            reportInto_list = reportInfo_items
        else:
            reportInto_list.append(reportInfo_items)
        for reportInfo in reportInto_list:
            print("main_report_request==2")
            print(reportInfo)
            try:
                _report = self.get_report(report_info=reportInfo)
                print("main_report_request==2==1")
                self.update_state(report_info=reportInfo)
                print("main_report_request==2==2")
                time.sleep(3)
                print("main_report_request==2==3")
                self.upload_report2s3(_report, end_date=reportInfo["EndDate"]["value"])
                print("main_report_request==2==4")
                self.update_uploadstate(report_info=reportInfo)
                print("main_report_request==2==5")
            except Exception as e:
                print("exception main_report_request==2==1 ")
                print(e)
            print("main_report_request==3")
            # self.process(start_date.strftime("%Y-%m-%d") + name + " " + request_id)

    def upload_report2s3(self, report, end_date):
        # Split the long string into a list of lines
        bucket_name = "optivations-allorders"
        file_name = end_date
        s3_path = "AllOrders-{}/{}".format(self.account["merchant_name"], file_name)
        s3 = boto3.resource("s3")
        data = report.original.decode('utf-8')
        s3.Bucket(bucket_name).put_object(Key=s3_path, Body=data)

    def get_report(self, report_info):
        report_id = report_info["GeneratedReportId"]["value"]
        print("get_report ={}".format(report_id))
        report = self.reportClient.get_report(report_id)
        print("get_report finished ", report)
        return report

    def update_uploadstate(self, report_info):
        mws_reports.update_item(
            Key={
                "request_id": report_info["ReportRequestId"]["value"]
            },
            UpdateExpression="SET upload_status=:value",
            ExpressionAttributeValues={
                ":value": "Uploaded",
            },
        )

    def update_state(self, report_info):
        print("update_state =[{}]=[{}],[{}]".format(
            report_info["ReportRequestId"]["value"],
            report_info["ReportProcessingStatus"]["value"],
            report_info["GeneratedReportId"]["value"]
        ))
        mws_reports.update_item(
            Key={
                "request_id": report_info["ReportRequestId"]["value"]
            },
            UpdateExpression="SET mws_report_status=:value, report_id=:value_report",
            ExpressionAttributeValues={
                ":value": report_info["ReportProcessingStatus"]["value"],
                ":value_report": report_info["GeneratedReportId"]["value"]
            },
        )


def get_merchant_list():
    developer_name = os.environ["DEVELOPER_KEYS"]
    developer_keys = dynamodb.Table(developer_name)
    params = {'Select': "ALL_ATTRIBUTES"}
    dev_response = developer_keys.scan(**params)
    developer_info = {}
    for dev in dev_response["Items"]:
        developer_info[dev["developer_id"]] = {
                "access_key": dev["access_key"],
                "secret_key": dev["secret_key"],
            }
    client_keys_name = os.environ["CLIENT_KEYS"]
    table = dynamodb.Table(client_keys_name)
    response = table.scan(**params)
    print(response['Items'])
    print(developer_info)
    if response['Count'] > 0:
        return response['Items'], developer_info

    return None

def lambda_handler(event, context):
    # TODO implement

    merchants, dev = get_merchant_list()
    if merchants is not None:
        for _merchant in merchants:
            try:
                _merchant["access_key"] = dev[_merchant["developer_id"]]["access_key"]
                _merchant["secret_key"] = dev[_merchant["developer_id"]]["secret_key"]
                getreport = GetReport(_merchant)
                getreport.get_report_state()
            except Exception as e:
                print(e)
                pass
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

if __name__ == '__main__':
    # merchants, dev = get_merchant_list()
    # if merchants is not None:
    #     for _merchant in merchants:
    #         _merchant["access_key"] = dev[_merchant["developer_id"]]["access_key"]
    #         _merchant["secret_key"] = dev[_merchant["developer_id"]]["secret_key"]
    #         getreport = GetReport(_merchant)
    getreport = GetReport(config)
    report_info = {
        "GeneratedReportId": {
            "value": "22585067345018493"
        }
    }
    getreport.get_report(report_info)

    getreport.main_report_request(request_id=("102575018493", "102572018493", "102573018493", "102574018493"))
    # getreport.main_report_request(request_id=("601825018494"))


