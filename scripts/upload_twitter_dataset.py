import boto3
import os
import sys

BUCKET_NAME   = os.environ.get("BRONZE_BUCKET_NAME", "social-media-data-lake")
DATASET_PATH  = sys.argv[1] if len(sys.argv) > 1 else "training.1600000.processed.noemoticon.csv"
S3_KEY        = "bronze/twitter/sentiment140.csv"

s3 = boto3.client("s3")

print(f"Uploading {DATASET_PATH} → s3://{BUCKET_NAME}/{S3_KEY}")
s3.upload_file(DATASET_PATH, BUCKET_NAME, S3_KEY)
print("Done!")