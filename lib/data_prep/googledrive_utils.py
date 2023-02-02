import os
import io
# from google.auth.transport.requests import Request
# from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import dotenv
dotenv.load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/drive']

creds_path = os.path.join(os.getenv('PROJECT_PATH'), 'creds/credentials.json')
token_path = os.path.join(os.getenv('PROJECT_PATH'), 'creds/token.json')
service_account_path = os.path.join(os.getenv('PROJECT_PATH'), 'creds/service_account.json')

credentials = service_account.Credentials.from_service_account_file(service_account_path, scopes= SCOPES)
service = build('drive', 'v3', credentials=credentials)

def download_file_from_drive(file_id):
    request = service.files().get_media(fileId=file_id)
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print(F'Download {int(status.progress() * 100)}.')
    return file.getvalue()

def upload_file_to_drive(file_name, file_path):
    try:
        file_metadata = {'name': file_name}
        media = MediaFileUpload(file_path, resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print('Upload of {} sucessful'.format(file_name))
    except:
        print('Upload of {} unsucessful'.format(file_name))
# creds = None
# if os.path.exists(token_path):
#     creds = Credentials.from_authorized_user_file(token_path, SCOPES)
# # If there are no (valid) credentials available, let the user log in.
# if not creds: print('not creds')
# if not creds.valid: print('not creds.valid')
# if not creds or not creds.valid:
#     if creds and creds.expired and creds.refresh_token:
#         creds.refresh(Request())
#     else:
#         flow = InstalledAppFlow.from_client_secrets_file(creds_path, scopes=SCOPES)
#         creds = flow.run_local_server(port=0)
#     # Save the credentials for the next run
#     with open(token_path, 'w') as token:
#         token.write(creds.to_json(token_path))

# credentials = Credentials.from_authorized_user_file(creds)
# service = build('drive', 'v3', credentials=credentials)

# results = service.files().list(pageSize=10, fields="nextPageToken, files(id, name)").execute()

# items = results.get('files', [])

# print('Files:')
# for item in items:
#     if item['name'][-4:] == '.txt':
#         print('{} ({})'.format(item['name'], item['id']))
#         print(download_file_from_drive(item['id']))