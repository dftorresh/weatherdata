import requests
from google.cloud import storage
from datetime import datetime


def fetch_weather_data(appid, lat, lon, units, exclude=None):
    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={appid}&units={units}"
    url += f"&exclude={exclude}" if exclude else ""
    api_response = requests.get(url=url)
    api_response.raise_for_status
    if api_response.status_code != 200:
        raise Exception(f"Error when calling weather API. Error code {api_response.status_code}")
    return api_response.json()


def upload_data_to_bucket(weather_data, bucket_name, folder):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    current_date = datetime.now()
    formated_current_date = current_date.strftime("%Y%m%d_%H%M%S")
    json_file_name = f"{folder}/weather_data_{formated_current_date}.json"
    blob = bucket.blob(json_file_name)
    blob.upload_from_string(data=str(weather_data))
    return json_file_name


def extract_weather_data_into_bucket(appid, lat, lon,  units, exclude, bucket_name, folder):
    weather_data = fetch_weather_data(appid, lat, lon,  units, exclude)
    json_file_name = upload_data_to_bucket(weather_data, bucket_name, folder)
    return json_file_name

