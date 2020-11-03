# MWS-OrderReport
Get order report by MWS api periodly
##### 1 Get credential entries from dynamo db, call RequestReport for getting order Report
##### 2 After a while check GetReportRequestList to get `Done` state Request and get Report id from there, get Report
##### 3 Create a CSV file from Report and store it on S3.

####RequestReport.py
- This is the lambda function for sending request. Lambda pulls the credentials from `request_state` in dynamodb.
If there is no entries(that means first request), send three requests since 90 days ago.
If there is it sends request since the last time.
- After send the `done` field in table will be updated as `_SUBMITTED_` state, and also set `request_id`
AWS keys and other sensitive information are stored in dynamo and environment variables.

####GetReport.py
- This is the lambda function to get report and store them as S3 objects.
Get available `request_id` from table and get report from those id.
Finally store csv formatted result as S3 object.
