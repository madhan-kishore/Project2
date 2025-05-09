from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CleanAndMergeBankTxns") \
    .getOrCreate()

# Read all transaction files from GCS
df = spark.read.option("header", True).csv("gs://madhan-bucket/project2/*/*.csv")

# Drop rows with any null or empty string values
df_clean = df.dropna(how="any")
for column in df_clean.columns:
    df_clean = df_clean.filter(col(column) != '')

# Write cleaned data to GCS as merged CSV
df_clean.write.option("header", True).mode("overwrite") \
    .csv("gs://madhan-bucket/cleaned/merged_transactions.csv")

print("✅ Cleaned and merged transactions saved to GCS.")
