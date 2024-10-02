import os
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv(override=True)

def get_gcs_bucket():
    service_account_path = os.path.join(os.getenv('PROJECT_PATH'), 'creds/service_account.json')
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    client = storage.Client(credentials=credentials, project='letterboxd-app')
    bucket = client.get_bucket('lb-db')
    return bucket

def upload_file(local_path, gcs_destination):
    bucket = get_gcs_bucket()
    gcs_object = bucket.blob(gcs_destination)
    gcs_object.upload_from_filename(local_path)

def download_file(gcs_path, local_destination):
    bucket = get_gcs_bucket()
    gcs_object = bucket.blob(gcs_path)
    gcs_object.download_to_filename(local_destination)

def download_db():
    try:
        download_file('lb-film.db', os.getenv('WORKING_DB'))
        print('db downloaded from GCS')
    except:
        print('db download failed')

def upload_db():
    try:
        upload_file(os.getenv('WORKING_DB'), 'lb-film.db')
        print('db uploaded to GCS')
    except:
        print('db upload failed')

def backup_db():
    today_formatted = datetime.today().strftime("%Y%m%d")
    try:
        upload_file(os.getenv('WORKING_DB'), 'backups/lb-film-{}.db'.format(today_formatted))
        print('db uploaded to GCS')
    except:
        print('db upload failed')