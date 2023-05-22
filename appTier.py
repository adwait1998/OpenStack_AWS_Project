from flask import Flask, request
import boto3
import base64
import uuid
import os
from datetime import datetime

AWS_REGION='us-east-1'

AWS_ACCESS_KEY="AKIAZJBPOCBNEKSQ7Z5S"
AWS_SECRET_ACCESS_KEY="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"

INPUT_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/637902131290/InputQueue"
INPUT_QUEUE_NAME="InputQueue"

session=boto3.session.Session()
sqs_res = session.resource("sqs", region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


app = Flask(__name__)

@app.route("/",methods = ['POST'])
def read_image_file():
    input_q = sqs_res.get_queue_by_name(QueueName=INPUT_QUEUE_NAME)

    upload_file = request.files['image_file']
    if upload_file.filename != '':
        upload_file.save("requests_files/" + upload_file.filename)
        with open("requests_files/" + upload_file.filename, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
            image_file.close()

        msg_uuid=str(uuid.uuid4())
        
        try:

            input_q.send_message(MessageBody=encoded_string, MessageAttributes={
            'ImageName': {
                'StringValue': upload_file.filename,
                'DataType': 'String'
            },
            'UID': {
                'StringValue': msg_uuid,
                'DataType': 'String'
            }
        })

        except Exception as e:
            print(f"Exception while sending message to SQS ::: " + upload_file.filename + " ::: {repr(e)}")

        print("Message sent for " + upload_file.filename + " at : ",datetime.now()) #.strftime("%H:%M:%S")

        os.system("rm requests_files/" + str(upload_file.filename))

    res=None

    while res is None:
        if os.path.exists("requests_files/" + msg_uuid + ".txt"):
            with open("requests_files/" + msg_uuid + ".txt") as file:
                res = file.read()
                print("Result for " + upload_file.filename + " : ",res)
            if res:
                os.system("rm requests_files/" + msg_uuid + ".txt")
                return res
        
    print("Exiting...")

if __name__ == "__main__":
    app.run()
