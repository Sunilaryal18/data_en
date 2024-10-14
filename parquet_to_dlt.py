import dlt
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Parquet to Delta Live Tables") \
    .getOrCreate()

# Define the path to the Parquet file (source)
# Update with your actual path for Azure or AWS
# For Azure Blob Storage (source)
source_parquet_path = "dbfs:/mnt/source_mount_point/output_data.parquet"  # Update with your actual DBFS path

# For AWS S3 (source)
# source_parquet_path = "s3a://your_source_bucket_name/output_data.parquet"  # Uncomment and update if using S3

# Define the path to save the Delta table (destination)
# For Azure Blob Storage (destination)
destination_delta_path = "dbfs:/mnt/destination_mount_point/your_delta_table_name"  # Update with your actual DBFS path

# For AWS S3 (destination)
# destination_delta_path = "s3a://your_destination_bucket_name/your_delta_table_name"  # Uncomment and update if using S3

# Define a Delta Live Table
@dlt.table(
    name="your_delta_table_name",  # Name of the Delta table
    comment="This table is created from a Parquet file."
)
def create_delta_table():
    # Read the Parquet file from the source storage account
    df = spark.read.parquet(source_parquet_path)  # Use the appropriate source path

    # Perform any transformations if needed
    # df = df.withColumn("new_column", df["existing_column"] + 1)  # Example transformation

    # Write the DataFrame to the destination Delta table
    df.write.format("delta").mode("overwrite").save(destination_delta_path)

    return df  # Return the DataFrame for DLT
