import os
import pandas as pd
import boto3
from io import StringIO

from dotenv import load_dotenv
load_dotenv("/path/to/.env", override=True)


def df_to_s3(df, bucket, key):
    # Create a session
    session = boto3.session.Session(profile_name=os.environ.get("AWS_SECRETS_PROFILE_NAME"))
    aws_s3_client = session.client(
        service_name="s3",
        region_name=os.environ.get("AWS_SECRETS_REGION_NAME"),
    )

    # Create a CSV string from the DataFrame
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Put the CSV string to S3
    aws_s3_client.put_object(
        Body=csv_buffer.getvalue(),
        Bucket=bucket,
        Key=key
    )
    print(f'Successfully put DataFrame to {bucket}/{key}')