from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName('Banking Transaction Failed Filter') \
    .getOrCreate()

# Define input and output paths
input_path = "gs://madhan-bucket/cleaned/merged_transactions.csv"
output_path = "gs://madhan-bucket/output/failed_transactions"

# Load the cleaned data
df = spark.read.option("header", "true").csv(input_path)

# Filter failed transactions (example: filtering rows where 'status' = 'FAILED')
failed_transactions = df.filter(df["status"] == "failed")

# Save the filtered data to a new CSV file in GCS
failed_transactions.coalesce(1).write.option("header", "true").csv(output_path)

# Stop the Spark session
spark.stop()
