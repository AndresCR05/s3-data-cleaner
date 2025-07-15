import boto3
import pandas as pd
from io import StringIO
from S3_functions import *
s3 = boto3.client('s3')

BUCKET_NAME = 'andres-data-bucket'  # bucket name
folder = 'sample-data/'              # local folder name 


# Menu for S3 operations
while True:
    key = "" # File in S3
    file_name = "" # Local File
    term = ""
    print("""
    📦 S3 Menu
    1. Upload file
    2. Download file
    3. Delete file
    4. List all files
    5. Read and show a CSV file
    6. Search for files by name
    7. List files sorted by date
    100. Exit
    """)
    try:
        number = int(input("Type a number: "))
    except ValueError:
        print("Please enter a valid number.")
        continue
    df = []
    match number: 
        case 1: 
            filename = folder + input("File Name: ")
            key = input("Name on S3: ")
            upload_file(s3, filename, BUCKET_NAME, key)
        case 2: 
            filename = folder + input("File Name: ")
            key = input("Name on S3: ")
            download_file(s3, BUCKET_NAME, key, filename)
        case 3:
            key = input("Name on S3: ")
            delete_file(s3, BUCKET_NAME, key)
        case 4:
            list_files(s3, BUCKET_NAME)
        case 5:
            key = input("Name on S3: ")
            df = read_csv_file(s3, BUCKET_NAME, key)
            print(df)
        case 6:
            term = input("Enter a search term or partial filename: ")
            search_files(s3, BUCKET_NAME, term)
        case 7:
            list_files_sorted_by_date(s3, BUCKET_NAME)
        case 100: 
            break
            