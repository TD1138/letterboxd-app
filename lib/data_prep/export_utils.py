import pandas as pd
import os
import shutil
import datetime
import time
from sqlite_utils import df_to_table
import dotenv
dotenv.load_dotenv()

def convert_uri_to_id(letterboxd_uri):
    return 'f_'+letterboxd_uri.replace('https://boxd.it/', '').zfill(5)

def unzip_letterboxd_downloads():
    raw_exports_folder = os.getenv('PROJECT_PATH')+'/db/raw_exports/'
    for file in os.listdir(raw_exports_folder):
        if file[-4:] == '.zip':
            file_path = os.path.join(raw_exports_folder, file)
            shutil.unpack_archive(file_path, file_path[:-4])
            time.sleep(5)
            os.remove(file_path)

def set_latest_export():
    latest_date = datetime.datetime.strptime('20200101', '%Y%m%d')
    letterboxd_user_name = os.getenv('LETTERBOXD_USER')
    letterboxd_exports_folder = os.path.join(os.getenv('PROJECT_PATH'), 'db/raw_exports')
    for i in os.listdir(letterboxd_exports_folder):
        if i[0] != '.':
            tmp = i.replace('letterboxd-'+letterboxd_user_name+'-', '')
            tmp_parsed = datetime.datetime.strptime(tmp, '%Y-%m-%d-%H-%M-%Z')
            if tmp_parsed > latest_date:
                latest_date = tmp_parsed
    latest_export_filename = 'letterboxd-' + letterboxd_user_name + '-' + datetime.datetime.strftime(latest_date, '%Y-%m-%d-%H-%M-%Z')+'utc'
    latest_export_file_loc = letterboxd_exports_folder + '/' + latest_export_filename
    dotenv.set_key(dotenv.find_dotenv(), 'LATEST_EXPORT', latest_export_file_loc)

def exportfile_to_df(export_filename):
    export_df = pd.read_csv(os.path.join(os.getenv('LATEST_EXPORT'), export_filename))
    export_df.columns = [x.upper().replace(' ', '_') for x in export_df.columns]
    return export_df

def refresh_core_tables():
    watched_df = exportfile_to_df('watched.csv')
    watched_df['FILM_ID'] = watched_df['LETTERBOXD_URI'].apply(convert_uri_to_id)
    df_to_table(watched_df[['FILM_ID']], 'WATCHED', replace_append='replace')
    watchlist_df = exportfile_to_df('watchlist.csv')
    watchlist_df['FILM_ID'] = watchlist_df['LETTERBOXD_URI'].apply(convert_uri_to_id)
    watchlist_df.columns = ['ADDED_DATE', 'NAME', 'YEAR', 'LETTERBOXD_URI', 'FILM_ID']
    df_to_table(watchlist_df[['FILM_ID', 'ADDED_DATE']], 'WATCHLIST', replace_append='replace')
    all_films_df = pd.concat([watched_df, watchlist_df])
    all_films_df['FILM_URL_TITLE'] = ''
    title_df = all_films_df[['FILM_ID', 'NAME', 'FILM_URL_TITLE', 'LETTERBOXD_URI']]
    title_df.columns = ['FILM_ID', 'FILM_TITLE', 'FILM_URL_TITLE', 'LETTERBOXD_URL']
    df_to_table(title_df, 'FILM_TITLE', replace_append='replace')
    watched_df = exportfile_to_df('watched.csv')