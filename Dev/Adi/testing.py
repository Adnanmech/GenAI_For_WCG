
from utils import *

 
# Example usage:
# Replace these with your AWS credentials and S3 bucket information
bucket_name = 'your-bucket-name'
s3_prefix = 'your/s3/directory/'
aws_access_key_id = 'your-access-key-id'
aws_secret_access_key = 'your-secret-access-key'
storage_path = 'index.json'

process_and_store_documents(bucket_name, s3_prefix, aws_access_key_id, aws_secret_access_key, 'us-east-1', storage_path)
