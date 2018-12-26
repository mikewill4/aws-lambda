from __future__ import print_function
from dynamodb_json import json_util as dbjson 
import boto3

print('Loading function')

def lambda_handler(event, context):
    print(event)
    sns = boto3.client(service_name="sns")
    message = dbjson.loads(event["Records"][0]["Sns"]["Message"])
    print(message)
    
    # process attrs to check validity and create ticket
    req_fields = ["title", "summary", "category", "event"]
    # Load event body json into dict
    load_event = dbjson.loads(message["event"])
    
    if set(req_fields).issubset(set(message.keys())) and "user" in load_event.keys():
        # Connect to ticketdb
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('Tickets')
        
        response = table.get_item(Key={"id": message["category"] + ":" + load_event["user"]})
        
        if "Item" not in response.keys():
            print("about to push item")
            table.put_item(
                Item={
                    # Format json for ticketdb
                    "id": message["category"] + ":" + load_event["user"],
                    "description": message["title"] + ":" + message["summary"],
                    "body": load_event
                }
            )
            print("pushed, now exiting")
            return "Ticket created: " + message["category"] + ":" + load_event["user"]
        print("Duplicate ticket")
        return "Duplicate ticket not created"
    else:
        print("malformed event")
        return "Error: malformed event"
