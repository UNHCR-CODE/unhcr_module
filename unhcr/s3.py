"""
Overview
    This Python file provides a function to list files within a specified folder in an AWS S3 bucket. It uses the boto3 library to 
    interact with the S3 service. The file is configured with AWS credentials and bucket/folder information stored in a separate constants file.

Key Components
    s3_client: 
        An S3 client object initialized with AWS access and secret keys. This object is used to interact with the S3 service. 
        Credentials are imported from a constants file.
    list_files_in_folder(bucket_name, folder_name): 
        This function takes the bucket name and folder name as input. It uses s3_client.list_objects_v2() to retrieve a list of objects 
        within the specified folder. It then logs the name and size of each object found. 
        If no objects are found, it logs a message indicating this. Error handling is included to catch and log any exceptions during the process. 
        It relies on the BUCKET_NAME and FOLDER_NAME constants for default values.
    constants import: 
        This file imports necessary credentials (ACCESS_KEY, SECRET_KEY) and S3 location information (BUCKET_NAME, FOLDER_NAME) 
        from a separate constants module. This separation of credentials from the main code is a good security practice.
"""

import boto3

from unhcr import app_utils
from unhcr import constants as const

mods=[["app_utils", "app_utils"],["constants", "const"]]
res = app_utils.app_init(mods=mods, log_file="unhcr.s3.log", version="0.4.7", level="INFO", override=False)
logger = res[0]
if const.LOCAL:  # testing with local python files
    logger, app_utils, const = res

# from unhcr import constants as const


def list_files_in_folder(bucket_name, folder_name):
    """
    List files in a specific folder within the given S3 bucket.

    Args:
        bucket_name (str): Name of the S3 bucket.
        folder_name (str): Name of the folder within the bucket.

    Raises:
        Exception: If there is an issue connecting to S3 or listing the files.
    """
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=const.ACCESS_KEY,
            aws_secret_access_key=const.SECRET_KEY,
        )
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
        if "Contents" in response:
            for item in response["Contents"]:
                logger.info(f"Item: {item['Key']}, Size: {item['Size']} bytes")
        else:
            logger.info(f"No files found in folder '{folder_name}'.")
    except Exception as e:
        logger.error(f"Error: {e}")
