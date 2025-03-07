import requests
from google.cloud import storage
from datetime import datetime


def fetch_weather_data(appid, lat, lon, units, exclude=None):
    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={appid}&units={units}"
    url += f"&exclude={exclude}" if exclude else ""
    try:
        api_response = requests.get(url=url)
        api_response.raise_for_status()
        return api_response.json()
    except requests.exceptions.HTTPError as e:
        custom_message = f"Weather API returned an error: {str(e)}"
        e.args = (custom_message,)
        raise
    except requests.exceptions.RequestException as e2:
        custom_message2 = f"Error calling weather API: {str(e2)}"
        e2.args = (custom_message2,)
        raise


def upload_data_to_bucket(weather_data, bucket_name, folder):
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
    except Exception as e:
        custom_message = f"Error accessing GCP bucket: {str(e)}"
        e.args = (custom_message,)
        raise
    current_date = datetime.now()
    formated_current_date = current_date.strftime("%Y%m%d_%H%M%S")
    json_file_name = f"{folder}/weather_data_{formated_current_date}.json"
    try:
        blob = bucket.blob(json_file_name)
        blob.upload_from_string(data=str(weather_data))
        return json_file_name
    except Exception as e2:
        custom_message2 = f"Error writing json file to GCP bucket: {str(e2)}"
        e2.args = (custom_message2,)
        raise


def extract_weather_data_into_bucket(appid, lat, lon,  units, exclude, bucket_name, folder):
    weather_data = fetch_weather_data(appid, lat, lon,  units, exclude)
    json_file_name = upload_data_to_bucket(weather_data, bucket_name, folder)
    return json_file_name