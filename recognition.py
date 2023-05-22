# importing libraries
from facenet_pytorch import MTCNN, InceptionResnetV1
import torch
from torchvision import datasets
from torch.utils.data import DataLoader
from PIL import Image
import csv
import os
import sys
import boto3
from io import BytesIO
import base64
import subprocess
from datetime import datetime
import torchvision.models as models
import torchvision.transforms as transforms
from urllib.request import urlopen
from PIL import Image
import numpy as np
import json
import time

mtcnn = MTCNN(image_size=240, margin=0, min_face_size=20) # initializing mtcnn for face detection
resnet = InceptionResnetV1(pretrained='vggface2').eval() # initializing resnet for face img to embeding conversion

def collate_fn(x):
    return x[0]

def classifier(url):
    print("URL: " + url)
    
    img = Image.open(url)
    print("image open")

    model = models.resnet18(pretrained=True)

    model.eval()
    img_tensor = transforms.ToTensor()(img).unsqueeze_(0)
    outputs = model(img_tensor)
    _, predicted = torch.max(outputs.data, 1)

    with open('./imagenet-labels.json') as f:
        labels = json.load(f)
    res = labels[np.array(predicted)[0]]
    print("result is: " + res)
    img_name = url.split("/")[-1]
    #save_name = f"({img_name}, {result})"
    save_name = f"{img_name},{res}"
    print(f"{save_name}")
    return save_name

def process_message(message,input_bucket,output_bucket):

    print(f"message id: {message['MessageId']}")
    
    image_n="default"
    uid="default"

    if message['MessageAttributes'] is not None:
        image_n = message['MessageAttributes']['ImageName']['StringValue']
        uid = message['MessageAttributes']['UID']['StringValue']
        print("Image N: ",image_n)
        print("UID: ",uid)

    #Save Image
    im2 = base64.b64decode(message['Body'])
    with open(image_n,"wb") as f:
        f.write(im2)

    if not os.path.exists(image_n):
        im = Image.open(BytesIO(base64.b64decode(message['Body'])))
        im.save(image_n)

    output = str(classifier('/home/ubuntu/classifier/' + str(image_n))) #check if image_name is the full path to the image
    name = output if output else 'default'

    print(name)

    #S3 Input
    try:
        name_file = str(image_n)
        input_objecttt = input_bucket.Object(name_file)
        with open(str(image_n), 'rb') as data:
            input_objecttt.upload_fileobj(data)
    except Exception as e:
        print(f"File upload to S3 Input Bucket : Fail ::: {repr(e)}")

    print("File upload to S3 Input Bucket : Success")

    #S3 Output
    name_file = str(image_n).split(".")[0]
    obj_output = output_bucket.Object(name_file)
    output_bucket_res = obj_output.put(Body=name)

    if output_bucket_res['ResponseMetadata']['HTTPStatusCode'] == 200:
        print("File upload to S3 Output Bucket : Success")
    else:
        print("File upload to S3 Output Bucket : Fail")


    os.system("rm " + str(image_n))

    result = {
        'ImageName' : image_n,   # test_00.jpg
        'Name' : str(name).split(",")[1],              # bathtub
        'UID': uid                  # UID_001
    }

    return result


def send_message(sqs,result,output_queue_url):

    sqs.send_message(
        QueueUrl=output_queue_url,
        DelaySeconds=10,
        MessageAttributes={
            'ImageName': {
                    'StringValue': result['ImageName'],
                    'DataType': 'String'
                },
                'UID': {
                    'StringValue': result['UID'],
                    'DataType': 'String'
                }
        },
        MessageBody=(result['Name'])
    )

    print("Msg sent to Output Queue")


if __name__ == "__main__":

    AWS_REGION='us-east-1'

    AWS_ACCESS_KEY=""
    AWS_SECRET_ACCESS_KEY=""

 

    INPUT_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/637902131290/InputQueue"
    OUTPUT_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/637902131290/OutputQueue"

    INPUT_BUCKET="inputbucket117"
    OUTPUT_BUCKET="outputbucket117"

    MESSAGE_ATTRIBUTES=['ImageName','UID']

    sqs = boto3.client("sqs",region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    s3 = boto3.resource("s3",region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    input_bucket = s3.Bucket(INPUT_BUCKET)
    output_bucket = s3.Bucket(OUTPUT_BUCKET)

    while True:

        response = sqs.receive_message(
            QueueUrl=INPUT_QUEUE_URL,
            AttributeNames=MESSAGE_ATTRIBUTES,
            MaxNumberOfMessages=10,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=30
        )
        if 'Messages' in response:
            print("Messages received at : ", datetime.now()) 
            for message in response['Messages']:
                try:
                    print("============================")
                    result=process_message(message,input_bucket,output_bucket)
                except Exception as e:
                    print(f"Exception while processing message: {repr(e)}")
                    continue

                try:
                    send_message(sqs,result,OUTPUT_QUEUE_URL)
                    print("============================")
                except Exception as e:
                    print(f"Exception while sending message: {repr(e)}")
                    continue
                
                try:
                    receipt_handle = message['ReceiptHandle']
                    sqs.delete_message(
                        QueueUrl=INPUT_QUEUE_URL,
                        ReceiptHandle=receipt_handle
                    )
                except Exception as e:
                    print(f"Exception while deleting message: {repr(e)}")
                    continue
