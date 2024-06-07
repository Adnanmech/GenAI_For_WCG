from llamaindex import GPTVectorStoreIndex, SimpleDirectoryReader, Document
import os
import json
import boto3
import fitz  # PyMuPDF
import pandas as pd
from io import BytesIO

def load_documents_from_s3(bucket_name, s3_prefix, aws_access_key_id, aws_secret_access_key, region_name='us-east-1'):
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    # List files in the specified S3 bucket directory
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
    if 'Contents' not in response:
        return []

    documents = []

    for obj in response['Contents']:
        file_key = obj['Key']
        # Retrieve file metadata
        metadata = s3_client.head_object(Bucket=bucket_name, Key=file_key)['Metadata']

        # Get the file content
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = file_obj['Body'].read()

        # Determine the file type and read content accordingly
        if file_key.endswith('.pdf'):
            doc = fitz.open(stream=file_content, filetype="pdf")
            pdf_text = ""
            for page in doc:
                pdf_text += page.get_text()
            document = Document(text=pdf_text, metadata=metadata)
        elif file_key.endswith('.txt'):
            text_content = file_content.decode('utf-8')
            document = Document(text=text_content, metadata=metadata)
        elif file_key.endswith('.xlsx') or file_key.endswith('.xls'):
            excel_df = pd.read_excel(BytesIO(file_content))
            excel_text = excel_df.to_string()
            document = Document(text=excel_text, metadata=metadata)
        else:
            # Skip files with unsupported formats
            continue

        documents.append(document)

    # Load all documents into the SimpleDirectoryReader
    reader = SimpleDirectoryReader()
    reader.add_documents(documents)

    return reader

def transform_data(documents):
    transformed_documents = []
    for doc in documents:
        # Example transformation: lowercasing all text
        transformed_text = doc.text.lower()
        transformed_document = Document(text=transformed_text, metadata=doc.metadata)
        transformed_documents.append(transformed_document)
    return transformed_documents

def index_documents(transformed_documents):
    index = GPTVectorStoreIndex.from_documents(transformed_documents)
    return index

def store_index(index, storage_path):
    # Serialize the index to a file
    with open(storage_path, 'w') as f:
        json.dump(index.serialize(), f)

def process_and_store_documents(bucket_name, s3_prefix, aws_access_key_id, aws_secret_access_key, region_name='us-east-1', storage_path='index.json'):
    # Load documents from S3
    reader = load_documents_from_s3(bucket_name, s3_prefix, aws_access_key_id, aws_secret_access_key, region_name)
    documents = reader.documents

    # Transform the data
    transformed_documents = transform_data(documents)

    # Index the documents
    index = index_documents(transformed_documents)

    # Store the index
    store_index(index, storage_path)

    print(f"Index stored at {storage_path}")

# Example usage:
# Replace these with your AWS credentials and S3 bucket information
bucket_name = 'your-bucket-name'
s3_prefix = 'your/s3/directory/'
aws_access_key_id = 'your-access-key-id'
aws_secret_access_key = 'your-secret-access-key'
storage_path = 'index.json'

process_and_store_documents(bucket_name, s3_prefix, aws_access_key_id, aws_secret_access_key, 'us-east-1', storage_path)
