import json
import boto3
import base64
import os

dynamo_db = boto3.resource('dynamodb')
firehose = boto3.client('firehose')
table = dynamo_db.Table("iserikov_suspicious_ips")

def lambda_handler(event, context):
    # TODO implement
    actual_items = []
    
    print (event)
    ''' 
    {
      "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "1",
                "sequenceNumber": "49590338271490256608559692538361571095921575989136588898",
                "data": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0Lg==",
                "approximateArrivalTimestamp": 1545084650.987
            },
            "eventSource": "aws:kinesis",
      ...
    '''
    if 'Records' in event:
        for record in event['Records']:
            #Kinesis data is base64 encoded so decode here
            payload=base64.b64decode(record["kinesis"]["data"])
            record = json.loads(payload)
            print("Decoded payload: " + str(payload))
       
            is_suspicious = table.get_item(Key={'suspicious_ip': record['user_ip']})
	    ''' table.get_item return:
		{'Item': {'name': 'igor2', 'suspicious_ips': '127.0.0.2'},
 		'ResponseMetadata': {'RequestId': 'TULCMJVAPQ3ST878V1U02139MJVV4KQNSO5AEMVJF66Q9ASUAAJG',
		  'HTTPStatusCode': 200, ...
	    '''
            if 'Item' not in is_suspicious:
                output_record = {
                    'Data': json.dumps(record) + '\n'
                }
                actual_items.append(output_record)

    
    if len(actual_items) > 0:
        response = firehose.put_record_batch(
            DeliveryStreamName='iserikov-firehose',
            Records=actual_items
            )
    else:
        return None
        
    return response
