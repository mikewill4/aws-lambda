from __future__ import print_function
from dynamodb_json import json_util as dbjson 

import json
import boto3

print('Loading function')


def lambda_handler(event, context):
    print("live")
    sns = boto3.client(service_name="sns")
    
    # process attrs to check validity and create ticket
    json_d = json.loads(event)
    req_fields = ["title", "summary", "category", "event"]
    
    if set(req_fields).issubset(set(json_d.keys())) and "user" in json_d["event"]:
        # format ticket from event
        ticket = {"id":json_d["category"] + ":" + json_d["event"]["user"], \
                  "description":json_d["title"] + ":" + json_d["summary"], \
                  "body":json_d["event"]}
        
        # Format put request for ticketdb
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('Tickets')
        #converted_json = dbjson.dumps(json.dumps(event))
        table.put_item(
            Item= ticket
        )
        #output = "{ \"Tickets\": [ { \"PutRequest\": { \"Item\": "
        #output += dbjson.dumps(json_d["event"])
        #output += "}}]}"
        #outFile = open("data.json", "w")
        #outFile.write(output)
        
        # Then execute batch write
        # aws dynamodb batch-write-item --request-items file://data.json
       
        return ticket
    else:
        return {"invalid":"malformed event error"}
