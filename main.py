from email.message import Message

import boto3
from model.face_recognition import  face_match
import json
import os

SQS_REQUEST = 'https://sqs.us-east-1.amazonaws.com/474668424004/1229658367-req-queue'
SQS_RESPONSE = 'https://sqs.us-east-1.amazonaws.com/474668424004/1229658367-resp-queue'
S3_REQUEST = '1229658367-in-bucket'
S3_RESPONSE = '1229658367-out-bucket'
IMAGE_FOLDER = './faces/'

def process(img_name):
    img_path = os.path.join(IMAGE_FOLDER, img_name)
    s3 = boto3.client('s3')
    if not os.path.exists(IMAGE_FOLDER):
        os.makedirs(IMAGE_FOLDER)
    s3.download_file(S3_REQUEST, img_name, img_path)
    result = face_match(img_path)[0]
    return result



def read_message_from_sqs():
    sqs = boto3.client('sqs')
    response = sqs.receive_message(
        QueueUrl=SQS_REQUEST,
        AttributeNames=['All'],
        MaxNumberOfMessages=1,
        MessageAttributeNames=['All'],
        VisibilityTimeout=60,
        WaitTimeSeconds=20
    )
    message = response.get('Messages', [])

    if not message:
        return None

    message = message[0]
    receipt_handle = message['ReceiptHandle']
    body = message['Body']
    img_name = body.split(":")[0]
    img_uuid = body.split(":")[1]
    face = process(img_name)

    response = sqs.send_message(
        QueueUrl=SQS_RESPONSE,
        MessageBody=face+":"+img_uuid,
        MessageAttributes={
            'Image':{
                'DataType': 'String',
                'StringValue': img_name
            }
        }
    )

    s3 = boto3.client('s3')

    data = {
        'img_name': img_name,
        'face': face
    }
    json_data = json.dumps(data)

    s3.put_object(Bucket=S3_RESPONSE, Key=img_name.split('.')[0], Body=json_data, ContentType='application/json')

    sqs.delete_message(
        QueueUrl=SQS_REQUEST,
        ReceiptHandle=receipt_handle
    )

    return


if __name__ == '__main__':
    while True:
        read_message_from_sqs()