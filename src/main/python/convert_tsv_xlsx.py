#!/usr/bin/env python

import pandas as pd
import sys
import os
from google.cloud import storage


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to a Google Cloud Storage bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    with open(source_file_name, 'rb') as f:
        blob.upload_from_file(f)




if __name__ == "__main__":
    args = sys.argv

    input_file = args[1]
    output_file = args[2]

    validation_bucket = "cfr-legacy-table-validation"
    destination_blob = f"dev/{output_file}"

    # Read the tab-delimited file into a DataFrame
    df = pd.read_csv(input_file, sep="\t")

    # Convert the DataFrame to an Excel file
    df.to_excel(output_file, index=False)

    print(f"Data successfully converted to Excel file : {output_file}")

    upload_blob(bucket_name=validation_bucket, source_file_name=output_file, destination_blob_name=destination_blob)

    print(f"Data successfully uploaded to gcs bucket: gs://{validation_bucket}/{destination_blob}")

