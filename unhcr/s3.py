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
        This file imports necessary credentials (ACCESS_KEY, SECRET_KEY) and S3 location information (BUCKET_NAME, FOLDER_NAME) used to 
        configure the S3 client and specify the target location. This suggests a practice of separating sensitive information from the 
        main code for better security and maintainability.
    Logging: 
        The code uses the logging module to provide information about the files found or any errors encountered. 
        This is crucial for monitoring and debugging.
"""

import boto3
import logging
from constants import ACCESS_KEY, SECRET_KEY, BUCKET_NAME, FOLDER_NAME

s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)


"""
    List files in a specific folder within the given S3 bucket.

    Args:
        bucket_name (str): Name of the S3 bucket.
        folder_name (str): Name of the folder within the bucket.

    Raises:
        Exception: If there is an issue connecting to S3 or listing the files.
"""

def list_files_in_folder(bucket_name, folder_name):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
        if 'Contents' in response:
            for item in response['Contents']:
                logging.info(f"Item: {item['Key']}, Size: {item['Size']} bytes")
        else:
            logging.info(f"No files found in folder '{folder_name}'.")
    except Exception as e:
        logging.error(f"Error: {e}")

###################################
# Hey there - I've reviewed your changes and found some issues that need to be addressed.

# Blocking issues:

# Avoid hardcoding AWS credentials directly in the code (e:_UNHCR\CODE\unhcr_module\unhcr\s3.py:6)
# Overall Comments:

# Storing AWS credentials in constants is not secure. Consider using AWS credentials provider chain, environment variables, or AWS IAM roles instead.
# The docstring is floating and not properly attached to the function it documents. Move it directly above the function definition.
# Here's what I looked at during the review
# 🟡 General issues: 1 issue found
# 🔴 Security: 1 blocking issue, 1 other issue
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:_UNHCR\CODE\unhcr_module\unhcr\s3.py:24

# suggestion(security): Use more specific exception handling instead of broad Exception catch
#         Exception: If there is an issue connecting to S3 or listing the files.
# """

# def list_files_in_folder(bucket_name, folder_name):
#     try:
#         response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
# Resolve
# e:_UNHCR\CODE\unhcr_module\unhcr\s3.py:6

# issue(security): Avoid hardcoding AWS credentials directly in the code
# import logging
# from constants import ACCESS_KEY, SECRET_KEY, BUCKET_NAME, FOLDER_NAME

# s3_client = boto3.client(
#     's3',
#     aws_access_key_id=ACCESS_KEY,
# Resolve
# e:_UNHCR\CODE\unhcr_module\unhcr\s3.py:29

# suggestion(code_refinement): Consider returning the file list instead of just logging it
#         response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
#         if 'Contents' in response:
#             for item in response['Contents']:
#                 logging.info(f"Item: {item['Key']}, Size: {item['Size']} bytes")
#         else:
#             logging.info(f"No files found in folder '{folder_name}'.")
