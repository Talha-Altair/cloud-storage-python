import os

from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']

AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

import boto3

s3_client = boto3.client('s3', 
                        aws_access_key_id=AWS_ACCESS_KEY_ID, 
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

def upload_file_to_s3() -> int:
    """
    Uploads file to s3 bucket
    """
    
    local_file_path, bucket_name, file_name = "static/a.txt" , "ctctalha" , "first-file.txt"

    s3_client.upload_file(local_file_path, bucket_name, file_name)

    return 0

def download_file_from_s3() -> int:
    """
    Downloads file from s3 bucket
    """

    download_file_path, bucket_name, file_name = "static/b.txt" , "ctctalha" , "first-file.txt"

    s3_client.download_file(bucket_name, file_name, download_file_path)

    return 0

from azure.storage.blob import BlobServiceClient

AZ_STORAGE_CONNECTION_STRING = os.environ['AZ_STORAGE_CONNECTION_STRING']

blob_service_client = BlobServiceClient.from_connection_string(AZ_STORAGE_CONNECTION_STRING)

def upload_to_az_container() -> int:
    """
    Uploads to azure blob container with blob name
    """

    local_file_path, container_name, blob_name = "static/a.txt", "ctctalha", "first-file.txt"

    container_client = blob_service_client.get_container_client(container_name)

    blob_client = container_client.get_blob_client(blob_name)

    with open(local_file_path, "rb") as data:

        blob_client.upload_blob(data, overwrite=True)

    return 0

def download_az_blob() -> int:
    """
    Downloads blob from azure blob container to local file
    """

    download_file_path, container_name, blob_name = "static/c.txt", "ctctalha", "first-file.txt"

    container_client = blob_service_client.get_container_client(container_name)

    blob_client = container_client.get_blob_client(blob_name)

    with open(download_file_path, "wb") as download_file:

        download_file.write(blob_client.download_blob().readall())

    return 0

from google.cloud import storage

storage_client = storage.Client.from_service_account_json('ctc-storage-svc-ac.json')

def upload_to_gcs_bucket() -> int:
    """
    Uploads to a GCS bucket
    """

    local_file_path, bucket_name, blob_name = "static/a.txt", "ctctalha", "first-file.txt"

    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.blob(blob_name)

    blob.upload_from_filename(local_file_path)

    return 0

def download_from_gcs_bucket() -> int:
    """
    Downloads from a GCS bucket
    """

    local_file_path, bucket_name, blob_name = "static/d.txt", "ctctalha", "first-file.txt"

    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.blob(blob_name)

    blob.download_to_filename(local_file_path)

    return 0

if __name__ == '__main__':

    # upload_file_to_s3()

    # download_file_from_s3()

    # upload_to_az_container()

    # download_az_blob()

    # upload_to_gcs_bucket()

    download_from_gcs_bucket()

    pass