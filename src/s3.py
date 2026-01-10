import io
import subprocess
import zipfile
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from src import consts

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
#Download updated DB from S3
def download_db_from_s3():
    print("Downloading DB from S3...")
    subprocess.run(["aws", "s3", "cp", consts.DB_S3_PATH, consts.DB_PATH], check=True)
    print("DB downloaded successfully.")


# ---------------- S3 helpers ----------------
#Return list of all S3 keys of archives
def list_archive_keys_all():
    archives = []
    kwargs = {"Bucket": consts.BUCKET, "Prefix": consts.S3_PREFIX}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        contents = resp.get("Contents") or []
        for obj in contents:
            key = obj["Key"]
            if key.endswith(".zip"):
                archives.append(key)
        if resp.get("IsTruncated"):
            kwargs["ContinuationToken"] = resp.get("NextContinuationToken")
            continue
        break
    #Sort by numbers in name (Episodes_output/{n}.zip)
    def key_to_num(k):
        try:
            name = k.replace(consts.S3_PREFIX, "")
            return int(name.replace(".zip", ""))
        except:
            return 0
    archives.sort(key=key_to_num)
    return archives


#Download zip-archive and return dict. Return empty is there is no archive
def download_archive_to_dict(s3_key):
    buf = io.BytesIO()
    try:
        s3.download_fileobj(consts.BUCKET, s3_key, buf)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey"):
            return {}
        raise
    buf.seek(0)
    files = {}
    with zipfile.ZipFile(buf, "r") as z:
        for name in z.namelist():
            files[name] = z.read(name)
    return files


def get_archive_filecount(s3_key):
    buf = io.BytesIO()
    try:
        s3.download_fileobj(consts.BUCKET, s3_key, buf)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey"):
            return 0
        raise
    buf.seek(0)
    with zipfile.ZipFile(buf, "r") as z:
        return len(z.namelist())


# Upload updated DB back to S3
def upload_db_to_s3():
    print("Uploading updated DB to S3...")
    subprocess.run(["aws", "s3", "cp", consts.DB_PATH, consts.DB_S3_PATH], check=True)
    print("DB uploaded successfully.")


def upload_to_s3(file_buffer: io.BytesIO, bucket_name: str, s3_key: str):
    """
    Uploads a file-like object (e.g., in-memory ZIP buffer) to S3.

    Args:
        file_buffer: The io.BytesIO object containing the data.
        bucket_name: The name of the S3 bucket (e.g., consts.BUCKET).
        s3_key: The destination S3 key/path.
    """
    try:
        print(f"Uploading file to S3: s3://{bucket_name}/{s3_key}")

        # Call the Boto3 client's method
        s3_client.upload_fileobj(
            Fileobj=file_buffer,
            Bucket=bucket_name,
            Key=s3_key
        )
        print("Upload successful.")
    except Exception as e:
        print(f"Error during S3 upload for key {s3_key}: {e}")


def upload_local_file_to_s3(local_file_path: str, bucket_name: str, s3_key: str):
    print(f"Starting upload of {local_file_path} to s3://{bucket_name}/{s3_key}")
    try:
        # The core Boto3 function to upload a file from the local filesystem
        s3_client.upload_file(
            Filename=local_file_path,
            Bucket=bucket_name,
            Key=s3_key
        )
        print(f"SUCCESS: Database file uploaded to S3 at: s3://{bucket_name}/{s3_key}")
    except NoCredentialsError:
        print("ERROR: AWS credentials not found. Check environment variables or configuration.")
        raise
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"ERROR: Bucket {bucket_name} not found.")
        else:
            print(f"ERROR: Client error during S3 upload: {e}")
        raise
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during S3 upload: {e}")
        raise