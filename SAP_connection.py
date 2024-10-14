import pandas as pd
import pyodbc
import boto3
from azure.storage.blob import BlobServiceClient

# SAP Database connection details
sap_server = 'your_sap_server'
sap_database = 'your_sap_database'
sap_username = 'your_username'
sap_password = 'your_password'
sap_query = 'SELECT * FROM your_table'  # Replace with your actual query

# Azure Blob Storage details
azure_connection_string = 'your_azure_connection_string'
azure_container_name = 'your_azure_container_name'
azure_blob_name = 'output_data.parquet'  # Name of the file to be saved in Blob Storage

# AWS S3 details
aws_access_key = 'your_aws_access_key'
aws_secret_key = 'your_aws_secret_key'
aws_bucket_name = 'your_aws_bucket_name'
aws_object_name = 'output_data.parquet'  # Name of the file to be saved in S3

# Function to extract data from SAP
def extract_data():
    conn_str = f'DRIVER={{SAP HANA ODBC Driver}};SERVER={sap_server};DATABASE={sap_database};UID={sap_username};PWD={sap_password};'
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(sap_query, conn)
    conn.close()
    return df

# Function to load data to Azure Blob Storage in Parquet format
def load_data_to_azure(df):
    parquet_file_path = '/tmp/output_data.parquet'  # Temporary file path
    df.to_parquet(parquet_file_path, index=False)

    blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
    blob_client = blob_service_client.get_blob_client(container=azure_container_name, blob=azure_blob_name)

    with open(parquet_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

# Function to load data to AWS S3 in Parquet format
def load_data_to_aws(df):
    parquet_file_path = '/tmp/output_data.parquet'  # Temporary file path
    df.to_parquet(parquet_file_path, index=False)

    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    s3_client.upload_file(parquet_file_path, aws_bucket_name, aws_object_name)

# Main ETL process
def main(storage_choice):
    # Extract
    data = extract_data()

    # Load based on user selection
    if storage_choice.lower() == 'azure':
        load_data_to_azure(data)
    elif storage_choice.lower() == 'aws':
        load_data_to_aws(data)
    else:
        print("Invalid storage choice. Please select 'azure' or 'aws'.")

if __name__ == "__main__":
    user_choice = input("Enter storage choice (azure/aws): ")
    main(user_choice)