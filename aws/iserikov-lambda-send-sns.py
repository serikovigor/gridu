## Serikov IO AWS Big Data course
# Kinesis Analitics->Labmda(send email)
#---------------------------
import json
import base64
import boto3

topic_arn = 'arn:aws:sns:us-east-1:571632058847:iserikov-bd-alert'
client = boto3.client('sns')

def lambda_handler(event, context):
    # TODO implement
    print('event' + str(event))
    ''' like:
    	{
	    "invocationId": "8a237820-cd14-40fd-b6ab-2b6728d29003", 
	    "records": [{"recordId": "e136c416-a6a4-49b1-a2d5-3d7987a476a9", 
            	"data": "eyJpdGVtX2lkIjoxMDM4LCJpdGVtc19jb3VudCI6MX0="}]
    
    	}
    '''
    if 'records' in event:
        for record in event['records']:
            #Kinesis data is base64 encoded so decode here
            payload=base64.b64decode(record["data"])
            record = json.loads(payload)
            print("Decoded payload: " + str(payload))
            try:
                message = str(record['items_id']) + ' has too many views'
                client.publish(TopicArn=topic_arn, Message=message, Subject='Rate Alarm')
                print('Successfully delivered alarm message')
            except Exception as e:
                print('Delivery failure: '+ str(e))
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
