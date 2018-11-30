from __future__ import print_function
from dynamodb_json import json_util as dbjson 

import json
import boto3

print('Loading function')


def lambda_handler(event, context):
    print(event)
    sns = boto3.client(service_name="sns")
    message = event["Records"][0]["Sns"]["Message"]

    # process attrs to check validity and create ticket
    req_fields = ["title", "summary", "category", "event"]
    # Load event body json into dict
    load_event = dbjson.dump(message["event"])
    if set(req_fields).issubset(set(message.keys())) and "user" in load_event.keys():
        # Connect to ticketdb
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('Tickets')
        
        print("about to push item")
        response = table.get_item(Key={"id": message["category"] + ":" + load_event["user"]})
        print(response)
        
        if "Item" not in response.keys():
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
