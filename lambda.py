def lambda_handler(event, context):
    print("Received event: ", json.dumps(event, indent=2))

    try:
        # Extract bucket name and object key from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        print(f"Bucket: {bucket_name}, Key: {object_key}")

        # Get the CSV file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        print(f"File retrieved from S3: {object_key}")

        file_content = response['Body'].read().decode('utf-8').splitlines()
        csv_reader = csv.reader(file_content)
        rows = list(csv_reader)
        print(f"Number of rows in file: {len(rows)}")

        # Define schema fields based on the file name
        schema_fields = []
        if "cast.csv" in object_key:
            schema_fields = ["movie_id", "actor_name", "role", "age"]
        elif "movie.csv" in object_key:
            schema_fields = ["movie_id", "title", "genre", "release_year", "rating"]
        elif "worldwide_production.csv" in object_key:
            schema_fields = ["movie_id", "production_country", "budget", "box_office", "currency"]
        else:
            raise ValueError(f"Unknown schema for file: {object_key}")

        print(f"Schema fields: {schema_fields}")

        # Clean the data
        cleaned_data = clean_data(rows, schema_fields)
        print(f"Number of cleaned rows: {len(cleaned_data)}")

        # Prepare the new key for the cleaned file
        cleaned_key = object_key.replace("raw/", "staging/")
        print(f"Saving cleaned data to: {cleaned_key}")

        # Write cleaned data to a new file in the staging/ folder
        output_buffer = io.StringIO()
        csv_writer = csv.writer(output_buffer)
        csv_writer.writerows(cleaned_data)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=cleaned_key,
            Body=output_buffer.getvalue()
        )

        print(f"Cleaned file saved to: {cleaned_key}")
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully processed and cleaned {object_key}')
        }

    except Exception as e:
        print(f"Error processing file {object_key}: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing file: {str(e)}')
        }
