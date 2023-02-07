import os
import awswrangler as wr
import boto3
from boto3.s3.transfer import TransferConfig
from zipfile import ZipFile, ZIP_DEFLATED


def read_data(file_object):
    while True:
        # read next line in file
        data = file_object.readline()

        # exit if no data read
        if len(data) == 0:
            break

        # get the first non-whitespace character from data
        first_char = data.lstrip()[0]

        # check for comment characters
        if first_char not in ["C", "c", "*"]:
            break

    return read_until_substring(data, " /")


def read_until_character(string, character):
    for i, ch in enumerate(string):
        if ch == character:
            return string[:i].strip()

    return string.strip()


def read_until_substring(string, substring):
    # convert tabs to spaces in string for simplicity
    string = string.replace("\t", " ")

    # find index of substring in string
    idx = string.find(substring)

    if idx:
        return string[:idx].strip()

    return string.strip()


def make_directory(path: str):
    """
    Make directory at specified location

    Parameters
    ----------
    path : str
        path to directory

        ..note:: This function will create intermediate directories if
                 they do not already exist

    Returns
    -------
    None
        directory is created if it does not already exist
    """
    if not os.path.exists(path):
        os.makedirs(path)


def zip_model(
    zip_name: str,
    model_path: str | list[str],
    exclude_keywords: list = [".git", ".exe", "bin", ".dll", ".ipynb_checkpoints"],
):
    """
    Add model files to zip archive

    Parameters
    ----------
    zip_name : str
        path and name of zip archive created

    model_path : str or list
        path to files to write to zip archive

    exclude_keywords : list

    Returns
    -------
    None
        writes files to zip archive
    """
    with ZipFile(zip_name, "w", compression=ZIP_DEFLATED, compresslevel=6) as zip:
        if isinstance(model_path, str):
            model_path = [model_path]

        for path in model_path:
            for root, _, files in os.walk(path):
                for file in files:
                    fpath = os.path.join(root, file)
                    zippath = os.path.relpath(fpath, start=path)

                    include_file = all([kw not in fpath for kw in exclude_keywords])
    
                    if include_file:
                        zip.write(fpath, zippath)


def get_file_size(path: str):
    """
    Return file size
    """
    size = os.path.getsize(path)
    
    GB = 1024 ** 3
    MB = 1024 ** 2
    KB = 1024

    if size > GB:
        size /= GB
        units = "GB"
    elif size > MB:
        size /= MB
        units = "MB"
    elif size > KB:
        size /= KB
        units = "KB"

    return size, units


def s3_upload(file_name: str):
    """
    Upload file to AWS S3 Bucket

    This function relies on an environment variable RESOURCE_BUCKET
    to specify an AWS S3 Bucket that already exists. If the environment
    variable does not exist, it will do nothing.

    Parameters
    ----------
    file_name: str
        path and name of file to upload to S3

    Returns
    -------
    None
    """
    base_name = os.path.basename(file_name)
    bucket = os.getenv("RESOURCE_BUCKET")

    if bucket:
        s3_path = f"s3://{bucket}/{base_name}"
        with open(file_name, "rb") as f:
            wr.s3.upload(local_file=f, path=s3_path)

def s3_download(file_name: str):
    """
    Download file from AWS S3 Bucket

    This function relies on an environment variable RESOURCE_BUCKET
    to specify an AWS S3 Bucket that already exists. If the environment
    variable does not exist, it will do nothing.

    Parameters
    ----------
    file_name : str
        path and name of file to download from S3

    Returns
    -------
    None
    """
    base_name = os.path.basename(file_name)
    bucket = os.getenv("RESOURCE_BUCKET")

    if bucket:
        s3_path = f"s3://{bucket}/{base_name}"
        with open(file_name, "wb") as f:
            wr.s3.download(path=s3_path, local_file=f)

def s3_upload2(file_name: str):
    """
    Upload file to AWS S3 Bucket

    This function relies on an environment variable RESOURCE_BUCKET
    to specify an AWS S3 Bucket that already exists. If the environment
    variable does not exist, it will do nothing.

    Parameters
    ----------
    file_name: str
        path and name of file to upload to S3

    Returns
    -------
    None
    """
    base_name = os.path.basename(file_name)
    bucket = os.getenv("RESOURCE_BUCKET")
    
    MB = 1024*1024
    config = TransferConfig(
        multipart_threshold=100*MB,
        max_concurrency=2,
    )

    if bucket:
        s3 = boto3.client('s3')
        with open(file_name, 'rb') as f:
            s3.upload_fileobj(f, bucket, base_name, Config=config)

def s3_download2(file_name: str):
    """
    Download file from AWS S3 Bucket using boto3
    
    This function relies on an environment variable RESOURCE_BUCKET
    to specify an AWS S3 Bucket that already exists. If the environment
    variable does not exist, it will do nothing.

    Parameters
    ----------
    file_name : str
        path and name of file to download from S3

    Returns
    -------
    None
    """
    base_name = os.path.basename(file_name)
    bucket = os.getenv("RESOURCE_BUCKET")
    
    MB = 1024*1024
    config = TransferConfig(
        multipart_threshold=100*MB,
        max_concurrency=2,
    )

    if bucket:
        s3 = boto3.client('s3')
        with open(file_name, 'wb') as f:
            s3.download_fileobj(bucket, base_name, f, Config=config)