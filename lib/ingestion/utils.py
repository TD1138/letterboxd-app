import os
import shutil
import time
import datetime
from dotenv import load_dotenv

load_dotenv()

def unzip_letterboxd_downloads():
    raw_exports_folder = os.getenv('PROJECT_PATH')+'/db/raw_exports/'
    for file in os.listdir(raw_exports_folder):
        if file[-4:] == '.zip':
            file_path = os.path.join(raw_exports_folder, file)
            shutil.unpack_archive(file_path, file_path[:-4])
            time.sleep(5)
            os.remove(file_path)

def find_latest_export():
    latest_date = datetime.datetime.strptime('20200101', '%Y%m%d')
    data_loc = os.getenv('DATA_PATH')
    letterboxd_user_name = os.getenv('LETTERBOXD_USER')
    letterboxd_exports_folder = os.path.join(os.getenv('PROJECT_PATH'), 'db/raw_exports')
    for i in os.listdir(letterboxd_exports_folder):
        if i[0] != '.':
            tmp = i.replace('letterboxd-'+letterboxd_user_name+'-', '')
            tmp_parsed = datetime.datetime.strptime(tmp, '%Y-%m-%d-%H-%M-%Z')
            if tmp_parsed > latest_date:
                latest_date = tmp_parsed
    latest_export_filename = 'letterboxd-' + letterboxd_user_name + '-' + datetime.datetime.strftime(latest_date, '%Y-%m-%d-%H-%M-%Z')+'utc'
    latest_export_file_loc = os.path.join(letterboxd_exports_folder, latest_export_filename)
    return latest_export_file_loc