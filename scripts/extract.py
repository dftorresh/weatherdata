import requests
from google.cloud import storage
from datetime import datetime


def extract_weather_data_into_bucket(appid, lat, lon, bucket_name, folder, units, exclude = None):
    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={appid}&units={units}"

    if exclude:
        url += f"&exclude={exclude}"
        
    current_date = datetime.now()
    formated_current_date = current_date.strftime("%Y%m%d_%H%M%S")
    output_json_file_name = f"{folder}/api_response_{formated_current_date}.json"

    api_response = requests.get(url=url)
    api_response.raise_for_status

    if api_response.status_code == 200:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(output_json_file_name)
        response_content = api_response.json()
        blob.upload_from_string(data=str(response_content))

    return output_json_file_name

