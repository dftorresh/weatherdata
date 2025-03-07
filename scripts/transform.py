from airflow import DAG
from google.cloud import storage
import json

def transform(**kwargs):
    task_instance = kwargs['ti']
    json_data_file = task_instance.xcom_pull(task_ids='extract')
    weather_data_dict = read_json_data_file(kwargs["bucket_name"], json_data_file)
    flatten_weather_data_dict(weather_data_dict)
    return weather_data_dict
    
# def read_cities_file():
#     storage_client1 = storage.Client()
#     bucket1 = storage_client1.get_bucket("airqualityproject28")
#     blob1 = bucket1.blob("data/cities.json")
#     json_cities = blob1.download_as_text()
#     cities_dict = json.loads(json_cities)
#     print(cities_dict)

def flatten_weather_data_dict(weather_data_dict):
    current_dict = weather_data_dict.pop("current")
    weather_dict = current_dict.pop("weather")
    current_dict.update(weather_dict[0])
    weather_data_dict.update(current_dict)

def read_json_data_file(bucket_name, json_file_path):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(json_file_path)
    json_data = blob.download_as_text()
    json_data = json_data.replace("'", "\"")
    json_data_dict = json.loads(json_data)
    return json_data_dict