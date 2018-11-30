from __future__ import print_function
from dynamodb_json import json_util as dbjson 

import json
import boto3

print('Loading function')


def lambda_handler(event, context):
    print(event)
    sns = boto3.client(service_name="sns")

    # process attrs to check validity and create ticket
    req_fields = ["title", "summary", "category", "event"]
    if set(req_fields).issubset(set(event.keys())) and "user" in event["event"].keys():
        # Connect to ticketdb
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('Tickets')
        
        # Load event body json into dict
        load_event = event["event"]
        print("about to push item")
        response = table.get_item(Key={"id": event["category"] + ":" + load_event["user"]})
        print(response)
        
        if "Item" not in response.keys():
            table.put_item(
                Item={
                    # Format json for ticketdb
                    "id": event["category"] + ":" + load_event["user"],
                    "description": event["title"] + ":" + event["summary"],
                    "body": event["event"]
                }
            )
            print("pushed, now exiting")
            return "Ticket created: " + event["category"] + ":" + load_event["user"]
        print("Duplicate ticket")
        return "Duplicate ticket not created"
    else:
        print("malformed event")
        return "Error: malformed event"
