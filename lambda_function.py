from __future__ import print_function
from dynamodb_json import json_util as dbjson 

import json
import boto3

print('Loading function')


def lambda_handler(event, context):
    print("live")
    sns = boto3.client(service_name="sns")
  
    # process attrs to check validity and create ticket
    req_fields = ["title", "summary", "category", "event"]
    if set(req_fields).issubset(set(event.keys())):
        # Connect to ticketdb
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('Tickets')
        
        # Load event body json into dict
        load_event = json.loads(event["event"])
        table.put_item(
            Item={
                # Format json for ticketdb
                "id": dbjson.dumps(event["category"] + ":" + load_event["user"]),
                "description": dbjson.dumps(event["title"] + ":" + event["summary"]),
                "body": dbjson.dumps(event["event"])
            }    
        )
        return "Ticket created: " + event["category"] + ":" + load_event["user"]
    else:
        return "Error: malformed event"
