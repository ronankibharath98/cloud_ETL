# AWS Data Pipeline with S3, Lambda, Glue, and Athena

This repository contains an AWS Glue ETL script to process and transform data from an S3 `staging` bucket and store it in an S3 `curated` bucket. The pipeline follows the Medallion Architecture.

---

## **Data Flow**
1. **S3 Raw**: Raw data is uploaded to the S3 bucket.
2. **Lambda Preprocessing**: A Lambda function cleans and preprocesses raw data, saving it to the `staging` bucket.
3. **S3 Staging**: Preprocessed data is stored in the staging layer.
4. **Glue ETL**: Transforms and cleans data from the staging layer and stores the curated data.
5. **S3 Curated**: Final cleaned and transformed data is stored in the curated layer.
6. **Glue Crawler**: Creates metadata in the Glue Data Catalog for querying in Athena.
7. **Athena**: Queries the curated data for analysis.

---

## **Setup Instructions**
### **1. Prerequisites**
- AWS account with access to S3, Glue, Lambda, and Athena.
- IAM role with necessary permissions for Glue jobs.
- Python installed locally for testing.

### **2. Upload Data to S3**
- Place raw CSV files in the `raw` folder of your S3 bucket.

### **3. Lambda Function**
- Deploy a Lambda function to clean and preprocess raw data, saving it to the `staging` bucket.

### **4. AWS Glue Job**
- Create an AWS Glue job:
  1. Go to the AWS Glue Console.
  2. Select "Jobs" and create a new job.
  3. Use the script provided in the `glue_etl_script.py` file.
  4. Specify the IAM role with necessary permissions.
  5. Schedule the job as required.

### **5. Glue Crawler**
- Set up a Glue Crawler to create a schema for the curated data:
  1. Go to the AWS Glue Console.
  2. Create a new Crawler.
  3. Set the data source as the `curated` bucket.
  4. Run the Crawler to populate the Glue Data Catalog.

### **6. Athena Setup**
- Query the curated data using Athena:
  1. Open the Athena Console.
  2. Select the database created by the Glue Crawler.
  3. Run SQL queries on the curated data.

---

## **File Structure**
