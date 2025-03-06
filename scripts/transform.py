from airflow import DAG
from google.cloud import storage

def transform(**kwargs): #bucket_name, json_data_file
    task_instance = kwargs['ti']
    json_data_file = task_instance.xcom_pull(task_ids='extract')
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(kwargs["bucket_name"])
    blob = bucket.blob(json_data_file)
    json_data = blob.download_as_text()
    return json_data