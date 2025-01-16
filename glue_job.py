import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col

# Initialize Glue context
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Define source and destination paths
staging_path = "s3://project-bigdata-bucket/staging/"
curated_path = "s3://project-bigdata-bucket/curated/"

# Step 1: Load data from S3
cast_df = spark.read.option("header", "true").csv(f"{staging_path}cast.csv")
movie_df = spark.read.option("header", "true").csv(f"{staging_path}movie.csv")
worldwide_production_df = spark.read.option("header", "true").csv(f"{staging_path}worldwide_production.csv")

# Step 2: Inner join movie_df and cast_df on movie_id
movie_cast_joined_df = movie_df.join(cast_df, "movie_id", "inner")

# Step 3: Inner join movie_cast_joined_df with worldwide_production_df on movie_id
final_df = movie_cast_joined_df.join(worldwide_production_df, "movie_id", "inner")

# Step 4: Drop duplicate columns if any
# Adjust column names to remove duplicates if necessary
final_df = final_df.dropDuplicates()

# Step 5: Write final data to the curated S3 bucket
final_df.write.mode("overwrite").csv(f"{curated_path}final_output/")

# Commit the job
job.commit()
