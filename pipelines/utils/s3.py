import boto3
import polars as pl
from io import StringIO, BytesIO
from dotenv import load_dotenv
import os

load_dotenv(override=True)

aws_access_key_id = os.getenv("COGNITO_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("COGNITO_SECRET_ACCESS_KEY")
region_name = os.getenv("COGNITO_REGION")

client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name,
)


def get_file(bucket_name: str, file_key: str) -> pl.DataFrame:
    s3_object = client.get_object(Bucket=bucket_name, Key=file_key)

    file_content = s3_object["Body"].read().decode("utf-8")

    return pl.read_csv(StringIO(file_content))


def drop_file(file_name: str, bucket_name: str, file_data: pl.DataFrame) -> None:
    csv_buffer = StringIO()

    file_data.write_csv(csv_buffer)

    csv_bytes = BytesIO(csv_buffer.getvalue().encode())

    client.upload_fileobj(csv_bytes, bucket_name, file_name)


def write_parquet(file_name: str, bucket_name: str, file_data: pl.DataFrame) -> None:
    parquet_buffer = BytesIO()

    file_data.write_parquet(parquet_buffer)

    parquet_buffer.seek(0)

    client.upload_fileobj(parquet_buffer, bucket_name, file_name)


def list_files(bucket_name: str):
    file_paths = []

    response = client.list_objects_v2(Bucket=bucket_name)

    for object in response["Contents"]:
        file_path = bucket_name + "/" + object["Key"]
        file_paths.append(file_path)

    return file_paths
