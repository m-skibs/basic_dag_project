# import requests
import pandas as pd
from datetime import datetime
import re


def fetch_data_from_rest_api() -> pd.DataFrame:
    # in a real scenario there would be an api_url param passed
    # in the dag the param would be populated by a config
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for unsuccessful responses
        data = response.json()
        return pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from REST API: {e}")
        return pd.DataFrame()
    """
    df = pd.read_json('/path/to/mock_api_resp.json')
    return df


def process_api_data(data) -> pd.DataFrame:
    # leaving as its own method in case CSV data is different from API data
    data['email_is_valid'] = data['email'].apply(validate_email)
    data['signup_date'] = data['signup_date'].apply(normalize_date)
    data['interests'] = data['interests'].apply(clean_interests)

    return data


def fetch_data_from_csv() -> pd.DataFrame:
    # if the CSV is in an object storage bucket, can implement this with corresponding cloud SDK
    # hardcoding in local csv path for example purposes, otherwise would pass as param
    csv_file_path = "/path/to/mock_input_csv.csv"
    try:
        data = pd.read_csv(csv_file_path)
        return data
    except FileNotFoundError:
        print(f"CSV file not found at {csv_file_path}")
        return pd.DataFrame()


def clean_interests(interests):
    if type(interests) == list:
        i = ','.join(interests)
    else:
        i = str(interests)
    i = i.replace(' "', '')
    i = i.replace('"', '')
    i = i.replace('[', '')
    i = i.replace(']', '')
    return i


def normalize_date(date_str):
    try:
        # Assume date_str is in 'YYYY-MM-DD' format
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        return None


def validate_email(email):
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def process_csv_data(data):
    # leaving as its own method in case CSV data is different from API data
    data['interests'] = data['interests'].apply(clean_interests)
    # create new column for whether email is valid, later can query users with valid emails
    # still want to store the info, regardless of whether email is valid
    data['email_is_valid'] = data['email'].apply(validate_email)
    data['signup_date'] = data['signup_date'].apply(normalize_date)

    return data
