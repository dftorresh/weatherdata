from google.cloud import storage

def transform(bucket_name, json_data_file):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(json_data_file)
    json_data = blob.download_as_text()
    return json_data