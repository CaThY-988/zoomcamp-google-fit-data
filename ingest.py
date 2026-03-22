import pandas as pd
from dotenv import load_dotenv
import os
import boto3

# Kaggle dataset available here: https://www.kaggle.com/datasets/aridoge13/google-fit-data
# V1 Approach - downloaded manually 
# V2 if time - set up kaggleAPI etc
df = pd.read_csv('data/raw/hamon_googlefit_medical_realistic.csv')
df.to_parquet('data/raw/hamon_googlefit_medical_realistic.parquet')

# Upload to S3
load_dotenv()
local_file_path = "data/raw/hamon_googlefit_medical_realistic.parquet"
bucket_name = "zoomcamp--497675597195-eu-west-2-an"
s3_key = "raw/googlefit/hamon_googlefit_medical_realistic.parquet"

s3 = boto3.client("s3")
s3.upload_file(local_file_path, bucket_name, s3_key)

print(f"Uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")

