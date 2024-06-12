from google.cloud import storage
from google.oauth2 import service_account
import os
from dotenv import load_dotenv

load_dotenv(override=True)

credentials = service_account.Credentials.from_service_account_file('service_account.json')
client = storage.Client(credentials=credentials, project='letterboxd-app')
bucket = client.get_bucket('lb-db')

def upload_file(local_path, gcs_destination):
    gcs_object = bucket.blob(gcs_destination)
    gcs_object.upload_from_filename(local_path)

def download_file(gcs_path, local_destination):
    gcs_object = bucket.blob(gcs_path)
    gcs_object.download_to_filename(local_destination)

def download_db():
    download_file('lb-film.db', os.getenv('WORKING_DB'))

def upload_db():
    upload_file(os.getenv('WORKING_DB'), 'lb-film.db')