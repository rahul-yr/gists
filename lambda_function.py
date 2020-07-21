import json
import urllib.parse
import boto3
import filetype
import os

# CUSTOM INPUTS
supported_formats = ['jpg','jpeg','mp4','png','pdf','zip','txt','mp3']
destination_bucket_name =os.environ['AWS_DESTINATION_BUCKET']


print('Loading function')

s3 = boto3.client('s3')

def delete_function(s3_client,bucket_name,key_name):
    deleteResponse = s3_client.delete_object(Bucket=bucket_name,Key=key_name)
    print('Deletion Response shown below')
    print(deleteResponse)


def lambda_handler(event, context):
    #1 - Get the bucket name
    bucket = event['Records'][0]['s3']['bucket']['name']

    #2 - Get the file/key name
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    copy_source_bucket_details = {
        'Bucket': bucket,
        'Key': key
    }
    try:
        #3 - Fetch the file from S3
        response = s3.get_object(Bucket=bucket, Key=key)

        data_type = response["Body"].read(1024)

        kind = filetype.guess(data_type)

        print("Bucket Name : {}\tKey : {}".format(bucket,key))        

        if kind is not None and response['ContentType'] == kind.mime:
            print('Same ContentType found')

            if kind.extension in supported_formats:
                print('Allowed ->\tBucket Name :{}\t| Key :{}\t| ContentType :{}\t'.format(bucket,key,kind.extension))
                print('Copying Files to Destination Bucket')

                # TODO Implement Security Check on ZIP FILES

                # Proceed to Copy
                extra_args = {
                    'ACL':'private'
                }
                s3.copy(copy_source_bucket_details, destination_bucket_name, key,extra_args)
                print('Copied Files to Destination Bucket & Deleting files from Source Bucket')
                print('Copied & Deleting ->\tSource_Bucket_Name :{}\t| Key :{}\t| ContentType :{}\t| Destination_Bucket_Name : {}'.format(bucket,key,kind.extension,destination_bucket_name))

                # Once copied Delete the file from Source bucket Delete the File
                delete_function(s3,bucket,key)

            else:
                print('Denied ->\tBucket Name :{}\t| Key :{}\t| ContentType :{}\t'.format(bucket,key,kind.extension))
                print('Not Supported format! Deleting the file..')
                
                # Implemented Delete the File
                delete_function(s3,bucket,key)
        else:
            print('Content Type Mismatch!. Deleting the file..')

            # Implemented Delete the file
            delete_function(s3,bucket,key)
        return 'Success!'

    except Exception as e:
        print(e)
        print('Some Error Occured! As precautionary deleting the file')
        delete_function(s3,bucket,key)
        raise e
        
