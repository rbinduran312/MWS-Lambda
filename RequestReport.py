import mws
import boto3
import os
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timezone
from datetime import timedelta

from Config import config

TABLE_REQUEST_STATE = "mws_reports"
TABLE_MACHANT = "merchants"

dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
mws_reports = dynamodb.Table(TABLE_REQUEST_STATE)

# table_mws = os.environ["MWS_REPORTS"]
# dynamodb = boto3.resource('dynamodb')
# mws_reports = dynamodb.Table(table_mws)


class ReportRequest:
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

    def make_query_data(self, _start):
        res_data = []
        _new_start = _start
        current_utc = datetime.utcnow()
        while True:
            _new_end = _new_start + timedelta(days=10)
            delta_offset = current_utc.replace(tzinfo=timezone.utc).timestamp() - _new_end.replace(
                tzinfo=timezone.utc).timestamp()
            if delta_offset < 0:
                _new_end = current_utc
            res_data_range = {
                "start_date": _new_start.strftime("%Y-%m-%dT%H:%M:%S"),
                "end_date": _new_end.strftime("%Y-%m-%dT%H:%M:%S")
            }
            res_data.append(res_data_range)
            _new_start = _new_end
            if delta_offset < 0:
                break
        return res_data

    def get_last_request(self):
        # _SUBMITTED_
        # _IN_PROGRESS_
        # _CANCELLED_
        # _DONE_
        # _DONE_NO_DATA_
        response = mws_reports.query(
            # _SUBMITTED_
            # _IN_PROGRESS_
            IndexName="pair_id-end_date-index",
            KeyConditionExpression=Key("pair_id").eq(self.account["pair_id"]),
            FilterExpression=Attr('mws_report_status').eq("_SUBMITTED_") | Attr('mws_report_status').eq(
                "_IN_PROGRESS_"),
            ScanIndexForward=False
        )
        if response['Count'] > 0:
            print("Still in processing")
            print(response['Items'])
            return
        response = mws_reports.query(
            IndexName="pair_id-end_date-index",
            KeyConditionExpression=Key('pair_id').eq(self.account['pair_id']),
            FilterExpression=Attr("mws_report_status").eq("_DONE_") | Attr("mws_report_status").eq(
                "_DONE_NO_DATA_") | Attr("mws_report_status").eq(
                "_CANCELLED_"),
            ScanIndexForward=False,
            Limit=10,
        )
        current_utc = datetime.utcnow()
        res_data = []
        if response['Count'] > 0:
            _info_merchant = response['Items'][0]
            if "end_date" in _info_merchant.keys():
                res_data_0 = {
                    "start_date": _info_merchant["end_date"]}
                _new_start = datetime.fromisoformat(_info_merchant["end_date"]).replace(tzinfo=timezone.utc)
                delta_offset = current_utc.replace(tzinfo=timezone.utc).timestamp() - _new_start.timestamp()
                if delta_offset / (60 * 60 * 24) > 10:  # still have to send for 30 days
                    if delta_offset / (60 * 60 * 24) > 90:
                        _new_start = current_utc - timedelta(days=90)
                    else:
                        _new_start = datetime.fromisoformat(_info_merchant["end_date"]).replace(tzinfo=timezone.utc)
                    res_data = self.make_query_data(_new_start)
                else:
                    _new_end = current_utc
                    _new_start = _info_merchant["end_date"]
                    res_data.append({
                        "start_date": _new_start.strftime("%Y-%m-%dT%H:%M:%S"),
                        "end_date": _new_end.strftime("%Y-%m-%dT%H:%M:%S")
                    })
            else:
                _new_start = current_utc - timedelta(days=90)
                res_data = self.make_query_data(_new_start)
        else:
            res_data = []
            _new_start = current_utc - timedelta(days=90)
            res_data = self.make_query_data(_new_start)
        return res_data

    def update_state(self, report_request):
        report_request_id = report_request["ReportRequestId"]

        new_item_resp = mws_reports.put_item(
            Item={
                "pair_id": self.account["pair_id"],
                "request_id": report_request_id["value"],
                "end_date": report_request["EndDate"]["value"],
                "start_date": report_request["StartDate"]["value"],
                "mws_report_status": report_request["ReportProcessingStatus"]["value"],
                "_status_": "Requested",
            }
        )
        return new_item_resp

    def request_report(self):
        time_info_s = self.get_last_request()
        print("request_report")
        if time_info_s is None:
            return

        print(time_info_s)
        for time_info in time_info_s:
            response = self.reportClient.request_report(report_type="_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_",
                                                        marketplaceids=(self.account["marketplace_id"]),
                                                        start_date=time_info["start_date"],
                                                        end_date=time_info["end_date"])
            try:
                if response.response.status_code == 200:
                    reportRequestInfo = response.parsed.ReportRequestInfo
                    self.update_state(report_request=reportRequestInfo)
            except Exception as e:
                print("Request_Report Exception {}".format(e))
                continue


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
                print("merchant==", _merchant)
                new_request = ReportRequest(_merchant)
                new_request.request_report()
            except Exception as e:
                print(e)
                pass
        return {
            'statusCode': 200,
            'body': json.dumps('Done!')
        }
    else:
        return {
            'statusCode': 400,
            'body': json.dumps('No available merchants!')
        }


if __name__ == '__main__':
    merchants, dev = get_merchant_list()
    if merchants is not None:
        for _merchant in merchants:
            _merchant["access_key"] = dev[_merchant["developer_id"]]["access_key"]
            _merchant["secret_key"] = dev[_merchant["developer_id"]]["secret_key"]
            new_request = ReportRequest(_merchant)
            new_request.request_report()
    # new_request.update_state(mock_reportRequest)
