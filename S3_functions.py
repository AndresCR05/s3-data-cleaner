import boto3
import pandas as pd
from io import StringIO


def download_file(s3, bucket, key, filename):
    try:
        s3.download_file(bucket, key, filename)
        print(f"File '{key}' downloaded as '{filename}'")
    except Exception as e:
        print("Error: ", e)
    
    
    
def delete_file(s3, bucket, key):
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        print(f"File '{key}' deleted from bucket '{bucket}'")
    except Exception as e:
        print("Error: ", e)
    
    
    
def list_files(s3, bucket):
    try:
        response = s3.list_objects_v2(Bucket=bucket)
        if 'Contents' in response:
            for obj in response['Contents']:
                print("📄", obj['Key'])
        else:
            print("Bucket is empty.")
    except Exception as e:
        print("Error: ", e)
            
            
            
def read_csv_file(s3, bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body'].read().decode('utf-8')  
        df = pd.read_csv(StringIO(body))
        return df
    except Exception as e:
        print("Error: ", e)
        return []



def put_object(df, s3, bucket, key):
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    except Exception as e:
        print("Error: ", e)
        
        
        
def upload_file(s3, filename, bucket, key):
    try:
        s3.upload_file(filename, bucket, key)
        print("File uploaded successfully!.")
    except Exception as e:
        print("Error uploading file:", e)
        
        
        
def search_files(s3, bucket, search_term):
    try:
        response = s3.list_objects_v2(Bucket=bucket)
        found = False
        if 'Contents' in response:
            for obj in response['Contents']:
                if search_term in obj['Key']:
                    print("🔎", obj['Key'])
                    found = True
        if not found:
            print("No files matched your search.")
    except Exception as e:
        print("Error: ", e)
    
    
    
def list_files_sorted_by_date(s3, bucket):
    try:
        response = s3.list_objects_v2(Bucket=bucket)
        if 'Contents' in response:
            sorted_files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
            print(f"\nFiles in bucket '{bucket}' (sorted by last modified):\n")
            for obj in sorted_files:
                date = obj['LastModified'].strftime("%Y-%m-%d %H:%M:%S")
                print(f"📄 {obj['Key']} — Last modified: {date}")
        else:
            print("Bucket is empty.")
    except Exception as e:
        print("Error: ", e)
