'''
Library import
'''

import boto3
import os

'''
Local libraries import
'''
import secrets
#import unifier


#from boto3.session import Session

#sess = Session(region_name='us-east-2')

s3 = boto3.resource(service_name=secrets.service_name,
                    region_name=secrets.region_name,
                    aws_access_key_id= secrets.aws_access_key_id,
                    aws_secret_access_key= secrets.aws_secret_access_key)
'''
Print all buckets
'''
#for bucket in s3.buckets.all():
#    print(bucket.name)

'''
Print all objects in bucket
'''

#for obj in s3.Bucket(secrets.aws_bucket).objects.all():
#    print(obj)
    
'''
Download files from s3 specific folder 
'''

def download_s3_folder (bucket_name, s3_folder, local_dir = None):
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix = s3_folder):
        target = obj.key if local_dir is None \
            else os.path.join(local_dir, os.path.relpath(obj.key, s3_folder))
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == '/':
            continue
        bucket.download_file(obj.key, target)
        
#download_s3_folder(secrets.aws_bucket, secrets.file_path,'./files')

