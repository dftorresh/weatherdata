from airflow import DAG
from google.cloud import storage
import json

def transform(**kwargs): #bucket_name, json_data_file
    task_instance = kwargs['ti']
    json_data_file = task_instance.xcom_pull(task_ids='extract')
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(kwargs["bucket_name"])
    blob = bucket.blob(json_data_file)
    json_data = blob.download_as_text()
    read_cities_file()
    return json_data


def read_cities_file():
    storage_client1 = storage.Client()
    bucket1 = storage_client1.get_bucket("airqualityproject28")
    blob1 = bucket1.blob("data/cities.json")
    json_cities = blob1.download_as_text()
    cities_dict = json.loads(json_cities)
    print(cities_dict)